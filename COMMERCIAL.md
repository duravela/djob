# djob licensing and commercial use

djob's engine is source-available under the [Duravela Internal Business Source
Available License 1.1](LICENSE). The [Go SDK](sdk/go/README.md), including its
worker client, handlers and worker protocol bindings, is separately licensed
under [Apache 2.0](sdk/go/LICENSE). The engine license expressly excludes it.
The engine is not open source. The SDK's Apache rights are not conditional on
accepting the engine license, and the engine has no automatic open-source
conversion date.

## Internal production use

Companies may evaluate, modify and operate the engine for their own development,
testing, staging and production, subject to the full license and applicable
third-party terms. This includes running it behind their own commercial
applications. A customer requesting an application-specific operation or
receiving its result does not by itself turn the application into an engine
offering.

| Example | Treatment under the project licenses |
| --- | --- |
| Run djob internally for maintenance, device readings or order processing | Permitted internal use |
| Sell a business application whose internal djob jobs generate customer reports | Permitted when customers receive application functionality, not a general execution platform |
| Hire a contractor to install and operate djob solely for your company's permitted internal use | Permitted service-provider use, including payment for that work |
| Sell SDK-only workers or libraries containing your handlers and drivers | Permitted under Apache 2.0 and the licenses of other code included |
| Provide customer-configurable general job submission, schedules, workflows or worker management backed by the engine | Separate commercial agreement required, including a free or renamed offering |
| Ship the engine in an appliance, customer installation or embedded product | Separate commercial agreement required |
| Resell engine binaries, images or modified engine distributions | Separate commercial agreement required; the license has a limited public-fork exception |
| Use engine materials for AI training | Separate license required; the engine license separately describes permitted AI-assisted development |

Service-provider permission covers work solely on behalf of a company exercising
its internal-use rights. It does not permit a provider to offer a shared hosted
engine platform to customers. Publishing a fork does not authorize distributing
engine packages or images through that fork.

## SDK and engine dependencies

The Apache exception covers the entire `sdk/go/` directory, including protocol
schemas and generated bindings. You may build, modify, distribute and sell
proprietary workers using that SDK, subject to Apache's conditions and any
third-party dependency obligations. Independently implementing its public
interfaces does not itself require an engine license.

SDK contributions follow Apache 2.0. Engine contributions follow the contribution
grant in Section 7.3 of the engine license, including its relicensing permission.
SDK and mixed submissions retain the separate treatment in Section 7.4.

The exception does not extend to code imported from `pkg/`, `internal/`, engine
commands or other engine directories. In particular, supplied protocol adapters
under `pkg/integrations/`, the supplied `djob-worker` executable and an embedded
controller built using `pkg/server` include engine code. An application that
imports both modules must comply with both sets of terms. Company examples
using optional engine integrations do not demonstrate an Apache-only binary.

The engine retains its license even though it incorporates Apache-licensed SDK
code. Conversely, an SDK-based worker does not acquire engine restrictions merely
by communicating with a controller.

## Outputs and AI use

Section 4 preserves ownership and permitted exploitation of your own materials
and factual outputs. Engine expression contained in an output retains its
applicable terms. Section 6 permits private AI assistance subject to its provider,
access, retention and reuse conditions. It also permits otherwise authorized AI
or ML workloads using materials you are entitled to use. Using engine materials
for model development requires separate permission unless independent rights
apply. The SDK remains governed by Apache 2.0.

## Commercial agreements

Contact Sebastian Kaiser, the Licensor identified in [LICENSE](LICENSE), through
the [djob repository](https://github.com/duravela/djob) before beginning a use
requiring a commercial agreement. Describe the legal entity, the proposed
offering, how customers interact with it, and whether engine code will be
distributed or operated for them. Do not include private commercial details in
a public issue; use the repository to request a private contact channel.

Approval, scope, compensation and any revenue participation must be expressly
agreed in writing. The public license specifies no automatic revenue-share
percentage and silence does not constitute approval. Duravela is the project
identity; creating a GitHub organization does not itself change the named
copyright holder or grant rights in third-party code.

This guide summarizes the project licenses. The license texts and any separate
written agreement govern. See [third-party notices](THIRD_PARTY_NOTICES.txt)
for upstream materials, the completed current-source dispositions and the
fresh-history publication procedure. The custom engine terms require legal review before
relying on them for public commercial licensing.
