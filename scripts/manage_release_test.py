import copy
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from contextlib import redirect_stdout

spec=importlib.util.spec_from_file_location('manage_release',Path(__file__).with_name('manage-release.py'))
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)

class Fake:
    origins=['http://127.0.0.1:8080']
    def __init__(self):
        self.definitions={'job':{'id':'job','revision':'1','runtime':'steps/v1','handler':'test','handler_version':'1','worker_release':'v1'}}
        self.triggers={'schedule':{'revision':'1','definition':{'id':'schedule','definition_id':'job','definition_revision':'1','event':{'source':'events'},'paused':True,'max_pending':1},'job_definition':copy.deepcopy(self.definitions['job'])}}
        self.sent=[];self.drop=False
    def deployment(self,tenant,id):return {'tenant_id':tenant,'id':id,'worker_id':id+'-worker','published':True,'state':'active','revision':'2','generation':'1','spec':{'image':'registry/worker@sha256:'+id,'replicas':1}}
    def all(self,tenant,kind):return [{'reference':{'id':id}} for id in (self.definitions if kind=='DEFINITION' else self.triggers)]
    def get(self,tenant,kind,id):return {kind.lower():copy.deepcopy((self.definitions if kind=='DEFINITION' else self.triggers)[id])}
    def request(self,path,body,origin=None):
        self.sent.append(body);q=json.loads(body)
        if self.drop:self.drop=False;raise OSError('receipt lost')
        key='put_definition' if 'put_definition' in q else 'put_trigger';kind='DEFINITION' if key=='put_definition' else 'TRIGGER';op=q[key]
        return {'request_id':q['command']['request_id'],'reference':{'kind':kind,'tenant_id':q['tenant_id'],'id':op['definition']['id']},'revision':str(int(op['expected_revision'])+1)}

class ReleasePlans(unittest.TestCase):
    def test_plan_is_read_only_and_schedules_pin_planned_revision(self):
        api=Fake();plan=m.plan(api,'alpha','v1','v2',allow_unqualified=True)
        self.assertEqual(api.sent,[]);self.assertEqual([op['kind'] for op in plan['operations']],['DEFINITION','TRIGGER'])
        self.assertEqual(plan['operations'][1]['value']['definition_revision'],'2')
        self.assertTrue(plan['operations'][1]['value']['paused'])
        self.assertEqual(plan['operations'][0]['before']['worker_release'],'v1')
    def test_trigger_only_does_not_rewrite_current_job(self):
        api=Fake();plan=m.plan(api,'alpha','v1','v2',allow_unqualified=True,triggers=['schedule'])
        self.assertTrue(plan['operations'][0]['id'].startswith('migration-'))
        self.assertEqual(plan['operations'][0]['before'],None)
        self.assertEqual(api.definitions['job']['worker_release'],'v1')
    def test_exact_lost_receipt_retry_and_confirmed_steps_not_resent(self):
        api=Fake();plan=m.plan(api,'alpha','v1','v2',allow_unqualified=True);api.drop=True
        with tempfile.TemporaryDirectory() as directory,redirect_stdout(io.StringIO()):
            path=Path(directory)/'plan.json';m.save(path,plan,exclusive=True)
            with self.assertRaises(OSError):m.apply(api,plan,path,m.digest(plan),allow_unobserved=True)
            with self.assertRaisesRegex(ValueError,'explicit'):m.apply(api,plan,path,m.digest(plan),allow_unobserved=True)
            m.apply(api,plan,path,m.digest(plan),retry_uncertain=True,allow_unobserved=True)
            self.assertEqual(api.sent[0],api.sent[1]);self.assertEqual(len(api.sent),3)
            m.apply(api,plan,path,m.digest(plan),allow_unobserved=True);self.assertEqual(len(api.sent),3)
            self.assertEqual(path.stat().st_mode&0o777,0o600)
    def test_rollback_reconstructs_original_specs_with_new_revisions(self):
        api=Fake();plan=m.plan(api,'alpha','v1','v2',allow_unqualified=True)
        api.definitions['job']['revision']='2';api.definitions['job']['worker_release']='v2';api.triggers['schedule']['revision']='2'
        state={'plan_digest':m.digest(plan),'operations':[{'status':'confirmed'}]*2}
        rollback=m.rollback(api,plan,state)
        self.assertEqual(rollback['operations'][0]['value']['worker_release'],'v1')
        self.assertEqual(rollback['operations'][0]['expected_revision'],'2')
        self.assertEqual(rollback['operations'][1]['value']['definition_revision'],'3')
    def test_rollback_refuses_uncertain_or_concurrently_edited_records(self):
        api=Fake();plan=m.plan(api,'alpha','v1','v2',allow_unqualified=True);state={'plan_digest':m.digest(plan),'operations':[{'status':'uncertain'},{'status':'waiting'}]}
        with self.assertRaisesRegex(ValueError,'uncertain'):m.rollback(api,plan,state)
        state['operations']=[{'status':'confirmed'}]*2
        with self.assertRaisesRegex(ValueError,'changed'):m.rollback(api,plan,state)
    def test_rollback_does_not_resume_release_when_no_configuration_was_promoted(self):
        api=Fake();plan=m.plan(api,'alpha','v1','v2',allow_unqualified=True)
        original_deployment=api.deployment
        def stopped(tenant,identity):
            value=original_deployment(tenant,identity)
            if identity=='v1':value['state']='retiring';value['spec']['replicas']=0
            return value
        api.deployment=stopped
        state={'plan_digest':m.digest(plan),'operations':[{'status':'waiting'}]*2}
        self.assertEqual(m.rollback(api,plan,state)['operations'],[])
        # A successfully created migration copy, before its trigger was changed,
        # likewise leaves nothing to restore and must not start the old release.
        plan=m.plan(api,'alpha','v1','v2',allow_unqualified=True,triggers=['schedule'])
        state={'plan_digest':m.digest(plan),'operations':[{'status':'confirmed'},{'status':'waiting'}]}
        self.assertEqual(m.rollback(api,plan,state)['operations'],[])

    def test_rollback_resumes_stopped_release_when_confirmed_configuration_needs_it(self):
        api=Fake();plan=m.plan(api,'alpha','v1','v2',allow_unqualified=True)
        api.definitions['job'].update(revision='2',worker_release='v2')
        api.triggers['schedule']['revision']='2'
        original_deployment=api.deployment
        def stopped(tenant,identity):
            value=original_deployment(tenant,identity)
            if identity=='v1':value['state']='retiring';value['spec']['replicas']=0
            return value
        api.deployment=stopped
        state={'plan_digest':m.digest(plan),'operations':[{'status':'confirmed'}]*2}
        result=m.rollback(api,plan,state)
        self.assertEqual([op['kind'] for op in result['operations']],['DEPLOYMENT','DEPLOYMENT','DEFINITION','TRIGGER'])
        self.assertEqual([op['value']['action'] for op in result['operations'][:2]],['scale','resume'])
        self.assertEqual([op['expected_revision'] for op in result['operations'][:2]],['2','3'])

    def test_health_gate_accepts_complete_current_capabilities_and_rejects_stale_or_partial(self):
        api=Fake();plan=m.plan(api,'alpha','v1','v2',allow_unqualified=True)
        class Observed(Fake):
            stale=False
            missing=False
            inventory_at=None
            foreign_runtime=False
            def request(self,path,body=None,origin=None):
                if path=='worker-deployments/status':
                    return {'tenant_id':'beta' if self.foreign_runtime else 'alpha','id':'v2','observed':True,'runtime':{'observed_at':'2000-01-01T00:00:00Z' if self.stale else m.now(),'state':'available','generation':'1','revision':'2','available_replicas':1}}
                return {'tenant_id':'alpha','observed_at':m.now() if self.inventory_at is None else self.inventory_at,'workers':[{'tenant_id':'alpha','deployment_id':'v2','deployment_generation':'1','state':'connected','runtimes':['steps/v1'],'handlers':[] if self.missing else [{'name':'test','version':'1'}],'drivers':[]}]}
        observed=Observed();m.preflight(observed,plan,False)
        report=m.readiness(observed,plan)
        self.assertTrue(report['ready']);self.assertEqual(report['plan_digest'],m.digest(plan))
        observed.stale=True
        report=m.readiness(observed,plan)
        self.assertFalse(report['ready']);self.assertEqual(len(report['checks']),3)
        self.assertTrue(report['checks'][0]['passed']);self.assertFalse(report['checks'][1]['passed'])
        with self.assertRaisesRegex(ValueError,'availability'):m.preflight(observed,plan,False)
        observed.stale=False
        for value in ['2000-01-01T00:00:00Z', '', 'invalid', '2099-01-01T00:00:00Z', '2026-01-01T00:00:00']:
            observed.inventory_at=value
            with self.assertRaisesRegex(ValueError,'availability'):m.preflight(observed,plan,False)
        observed.inventory_at=None;observed.foreign_runtime=True
        with self.assertRaisesRegex(ValueError,'availability'):m.preflight(observed,plan,False)
        observed.foreign_runtime=False;observed.missing=True
        with self.assertRaisesRegex(ValueError,'capabilities'):m.preflight(observed,plan,False)
        observed.missing=False;observed.definitions['job']['revision']='2'
        report=m.readiness(observed,plan)
        self.assertFalse(report['ready']);self.assertFalse(report['checks'][2]['passed'])
        self.assertEqual(observed.sent,[])
    def test_canary_gate_rejects_wrong_release_failed_and_stale_runs(self):
        api=Fake();deployment=api.deployment('alpha','v2');deployment['changed_at']=m.now()
        class Canary(Fake):
            def get(self,tenant,kind,identity):return {'run':copy.deepcopy(self.run)}
        canary=Canary();canary.run={'status':'succeeded','definition':{'worker_release':'v2'},'created_at':m.now(),'last_transition':m.now()}
        m.verify_canary(canary,'alpha','v2','canary',deployment)
        for field,value in [('status','failed'),('definition',{'worker_release':'v1'}),('created_at','2000-01-01T00:00:00Z'),('last_transition','2000-01-01T00:00:00Z')]:
            before=copy.deepcopy(canary.run);canary.run[field]=value
            with self.assertRaises(ValueError):m.verify_canary(canary,'alpha','v2','canary',deployment)
            canary.run=before
        with self.assertRaisesRegex(ValueError,'canary'):m.plan(api,'alpha','v1','v2')

    def test_reviewed_canary_launch_recovers_only_with_exact_explicit_retry(self):
        from types import SimpleNamespace
        class CanaryAPI(Fake):
            def request(self,path,body,origin=None):
                self.sent.append(body)
                if self.drop:self.drop=False;raise OSError('lost canary receipt')
                q=json.loads(body)
                return {'request_id':q['command']['request_id'],'reference':{'kind':'RUN','tenant_id':'alpha','id':q['launch']['run_id']}}
        api=CanaryAPI();api.definitions['job']['worker_release']='v2'
        with tempfile.TemporaryDirectory() as directory,redirect_stdout(io.StringIO()):
            args=SimpleNamespace(plan_file=str(Path(directory)/'canary.json'),approve=None,tenant='alpha',target='v2',definition=['job'],retry_uncertain=False)
            m.qualify(api,args);value=json.loads(m.private_read(args.plan_file));args.approve=m.digest(value);api.drop=True
            with self.assertRaises(OSError):m.qualify(api,args)
            with self.assertRaisesRegex(ValueError,'explicit'):m.qualify(api,args)
            args.retry_uncertain=True;m.qualify(api,args)
            self.assertEqual(api.sent[0],api.sent[1]);m.qualify(api,args);self.assertEqual(len(api.sent),2)

    def test_canary_http_failures_distinguish_rejection_from_uncertain_execution(self):
        from types import SimpleNamespace
        class CanaryAPI(Fake):
            def request(self,path,body,origin=None):
                self.sent.append(body)
                if self.drop:self.drop=False;raise OSError('lost canary receipt')
                raise m.urllib.error.HTTPError('http://127.0.0.1/mutate',self.code,'failure',{},None)
        # A later rejection cannot establish whether an earlier lost request ran.
        for lost,code,expected in [(False,400,'rejected'),(False,409,'rejected'),(False,408,'uncertain'),(False,503,'uncertain'),(True,409,'uncertain')]:
            with self.subTest(lost=lost,code=code),tempfile.TemporaryDirectory() as directory,redirect_stdout(io.StringIO()):
                api=CanaryAPI();api.code=code;api.definitions['job']['worker_release']='v2'
                args=SimpleNamespace(plan_file=str(Path(directory)/'canary.json'),approve=None,tenant='alpha',target='v2',definition=['job'],retry_uncertain=False)
                m.qualify(api,args);value=json.loads(m.private_read(args.plan_file));args.approve=m.digest(value)
                if lost:
                    api.drop=True
                    with self.assertRaises(OSError):m.qualify(api,args)
                    args.retry_uncertain=True
                with self.assertRaisesRegex(ValueError,'HTTP '+str(code)):m.qualify(api,args)
                state=json.loads(m.private_read(args.plan_file+'.state'))
                self.assertEqual(state['status'],expected);self.assertEqual(state['http_status'],code)
                self.assertTrue(all(wire==m.canonical(value['request']) for wire in api.sent))
                count=len(api.sent)
                if expected=='rejected':
                    args.retry_uncertain=True
                    with self.assertRaisesRegex(ValueError,'rejected'):m.qualify(api,args)
                else:
                    args.retry_uncertain=False
                    with self.assertRaisesRegex(ValueError,'explicit'):m.qualify(api,args)
                self.assertEqual(len(api.sent),count)

    def test_weight_transfer_preserves_other_parallel_releases(self):
        result=m.move({'id':'job','worker_release_weights':{'v1':5000,'v2':2000,'v3':3000}},'v1','v2')
        self.assertEqual(result['worker_release_weights'],{'v2':7000,'v3':3000})

if __name__=='__main__':unittest.main()
