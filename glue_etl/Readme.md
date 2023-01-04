# glue-etl

## Terraform

We want to manage glue crawlers, connections, databases and tables either via LakeFormation or via Terraform, not from the UI.


```sh
brew install terraform
terraform init
```

```sh
terraform plan
terraform apply
```

## Schedule the crawler

```sh
aws glue start-crawler --name dwh_discover
aws glue get-tables --database-name dwh
```
