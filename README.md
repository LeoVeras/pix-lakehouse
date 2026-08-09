# pix-lakehouse

Pipeline de ingestão de eventos PIX em arquitetura medalhão, com esteira
de deploy replicando o fluxo DSV → GMUD → PRD.

## Estrutura

```
databricks.yml           descrição do bundle e dos ambientes
resources/pix_job.yml    definição do job e suas tasks
src/                     notebooks
.github/workflows/       esteira de CI/CD
```

## A esteira

| Etapa | Onde acontece |
|---|---|
| Branch e PR | GitHub |
| Validações de código | `pr-automated.yml` |
| Merge na main | GitHub |
| Individual CI | `individual-ci.yml` |
| Deploy DSV | `bundle deploy -t dsv` |
| Testes funcionais | `bundle run` em DSV |
| GMUD | approval gate do Environment `gmud` |
| Deploy PRD | `bundle deploy -t prd` |

## Setup inicial

### 1. Secrets do repositório

`Settings → Secrets and variables → Actions → New repository secret`

- `DATABRICKS_HOST` — URL do workspace
- `DATABRICKS_TOKEN` — token de acesso pessoal

Gere o token em `Settings → Developer → Access tokens` no Databricks.

### 2. Environments

`Settings → Environments → New environment`

Crie três: `dsv`, `gmud`, `prd`.

No `gmud`, marque **Required reviewers** e adicione você mesmo.
É isso que faz a esteira parar e esperar aprovação — o análogo da GMUD.

### 3. Proteção da main

`Settings → Branches → Add rule` para `main`:

- Require a pull request before merging
- Require status checks to pass

Sem isso, dá pra commitar direto na main e furar a esteira inteira.

## O ciclo de trabalho

```bash
git checkout -b feature/ajuste-ingestao
# edita
git commit -am "ajusta parse do payload"
git push -u origin feature/ajuste-ingestao
```

Abre a PR no GitHub → validações rodam → aprova → merge →
esteira publica em DSV, roda os testes, e para na GMUD.
Você aprova na aba Actions e o deploy em PRD acontece.

## Regra que não se quebra

Artefato em produção nunca é editado à mão. Toda correção começa
no repositório e percorre a esteira de novo.

## Rollback

```bash
git revert <sha>
git push
```

A esteira roda de novo com a versão anterior. Rollback é deploy
para frente, não edição para trás.
