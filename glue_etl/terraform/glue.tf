resource "aws_glue_catalog_database" "dwh" {
  name = "dwh"
}

resource "aws_glue_connection" "dwh" {
  connection_properties = {
    JDBC_CONNECTION_URL = "jdbc:redshift://onefootball-dwh.c41sgik3szgd.eu-west-1.redshift.amazonaws.com:5439/onefootball"
    PASSWORD            = "Podolski10"
    USERNAME            = "admin"
  }

  name = "dwh"

  physical_connection_requirements {
    security_group_id_list = ["sg-78bb4f1e"]
    subnet_id              = "subnet-081b526d"
    availability_zone      = "eu-west-1a"
  }
}

resource "aws_glue_crawler" "dwh_discover" {
  name        = "dwh_discover"
  description = "Glue crawler to populate the Data Catalog with Redshift tables"

  jdbc_target {
    connection_name = "${aws_glue_connection.dwh.name}"
    path            = "onefootball/analytics/dim_localytics__users"
  }

  role = "${data.aws_iam_role.AWSGlueServiceRole.arn}"

  # schedule = 

  database_name = "${aws_glue_catalog_database.dwh.name}"
}
