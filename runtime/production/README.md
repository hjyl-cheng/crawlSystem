# Production Runtime

Production secrets, YouTube identities, and Rota source credentials live here
on the deployment host. They are intentionally excluded from Git and Docker
build contexts. Importing existing material requires a separately verified,
auditable migration; never copy values into an image.
