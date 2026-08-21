# GitHub Git LFS Limits and Quotas

Verified against GitHub's official documentation on 2026-08-21.

## Conclusion for a 126 MB object

Yes. A 126 MB file can be uploaded as a Git LFS object to a private GitHub
repository. GitHub blocks regular Git objects larger than 100 MiB, but directs
users to Git LFS for larger files.[1] A 126 MB LFS object is below the lowest
LFS per-object limit, 2 GB on GitHub Free and Pro.[2] Public and private
repositories use the same LFS accounting rules.[3]

This is conditional on the repository owner still having usable LFS storage,
or having a valid payment method and a budget that permits paid overage. The
upload counts against the repository owner's storage, not bandwidth.[3]

## Per-object maximum

| GitHub plan | Maximum LFS object |
| --- | ---: |
| GitHub Free | 2 GB |
| GitHub Pro | 2 GB |
| GitHub Team | 4 GB |
| GitHub Enterprise Cloud | 5 GB |

Objects above the applicable plan limit are rejected by Git LFS.[2]

## Included LFS usage

GitHub has removed the former prepaid LFS data-pack model and now uses metered
billing for usage above the included allowance.[3]

| GitHub plan | Included storage | Included download bandwidth per billing cycle |
| --- | ---: | ---: |
| GitHub Free | 10 GiB | 10 GiB |
| GitHub Pro | 10 GiB | 10 GiB |
| GitHub Free for organizations | 10 GiB | 10 GiB |
| GitHub Team | 250 GiB | 250 GiB |
| GitHub Enterprise Cloud | 250 GiB | 250 GiB |

Bandwidth is charged when LFS content is downloaded and its free allowance
resets each billing cycle. Uploads do not consume bandwidth. Storage is based
on all LFS objects associated with the repository, including the complete size
of every pushed version; storage charges are accrued hourly.[3]

## When quota or budget is exhausted

- Without a valid payment method, LFS usage is blocked after the included
  quota is consumed. If storage is over quota, clones retrieve pointer files
  rather than the LFS content and new LFS files cannot be pushed. If bandwidth
  is over quota, LFS is disabled until the next month.[3]
- With a valid payment method, usage above the included quota is metered and
  billed, subject to applicable budgets.[3]
- An LFS budget of `$0` prevents overage charges and blocks LFS for the rest of
  the calendar month; usage resets on the first of the next month. Deleting the
  budget removes the spending limit and bills all overage.[3]
- For a positive metered-product budget, enabling **Stop usage when budget
  limit is reached** blocks additional usage when any applicable hard budget is
  exhausted. Without that option, reaching the budget only triggers alerts;
  usage and billing continue.[4]

## Official sources

1. [About large files on GitHub](https://docs.github.com/en/repositories/working-with-files/managing-large-files/about-large-files-on-github)
2. [About Git Large File Storage](https://docs.github.com/en/repositories/working-with-files/managing-large-files/about-git-large-file-storage)
3. [Git Large File Storage billing](https://docs.github.com/en/billing/concepts/product-billing/git-lfs)
4. [Setting up budgets to control spending on metered products](https://docs.github.com/en/billing/how-tos/set-up-budgets)
