# https://www.terraform.io/docs/providers/aws/index.html

provider "aws" {
  version = "~>v2.17.0"
  region  = "eu-west-1"
}

terraform {
  backend "s3" {
    bucket = "terraform-states-production-of"
    key    = "glue.tfstate"
    region = "eu-west-1"
  }
}
