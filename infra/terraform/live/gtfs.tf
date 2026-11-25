# GTFS loader: one small Lambda, once a day. gtfs.de publishes about once a week; the loader
# skips versions already in bronze, so most runs only send two HEAD requests.
# Uses the same package as the poller (scripts/build_lambda.sh).

resource "aws_iam_role" "gtfs" {
  name               = "bahn-pulse-gtfs-loader"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

data "aws_iam_policy_document" "gtfs" {
  statement {
    sid = "ReadWriteBronzeGtfs"
    # GetObject is needed for HeadObject (is this version already stored?).
    actions   = ["s3:PutObject", "s3:GetObject"]
    resources = ["${module.bronze_bucket.arn}/gtfs/*"]
  }

  statement {
    # Without ListBucket, HeadObject on a missing key returns 403 instead of 404.
    # No s3:prefix condition: S3 does not set that key for HeadObject, so it would never match.
    sid       = "ListBronze"
    actions   = ["s3:ListBucket"]
    resources = [module.bronze_bucket.arn]
  }

  statement {
    sid       = "WriteLogs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.gtfs.arn}:*"]
  }
}

resource "aws_iam_role_policy" "gtfs" {
  name   = "gtfs-loader"
  role   = aws_iam_role.gtfs.id
  policy = data.aws_iam_policy_document.gtfs.json
}

resource "aws_cloudwatch_log_group" "gtfs" {
  name              = "/aws/lambda/bahn-pulse-gtfs"
  retention_in_days = 14
}

resource "aws_lambda_function" "gtfs" {
  function_name    = "bahn-pulse-gtfs"
  description      = "Loads new versions of the gtfs.de rail feeds (fv_free, rv_free) into bronze"
  role             = aws_iam_role.gtfs.arn
  runtime          = "python3.12"
  architectures    = ["arm64"]
  handler          = "bahn_pulse.ingestion.gtfs.handler"
  filename         = data.archive_file.poller.output_path
  source_code_hash = data.archive_file.poller.output_base64sha256
  memory_size      = 512 # holds one zip (~15 MB) in memory while validating it
  timeout          = 300

  environment {
    variables = {
      BRONZE_BUCKET = module.bronze_bucket.id
    }
  }

  depends_on = [aws_cloudwatch_log_group.gtfs, aws_iam_role_policy.gtfs]
}

resource "aws_lambda_function_event_invoke_config" "gtfs" {
  function_name          = aws_lambda_function.gtfs.function_name
  maximum_retry_attempts = 0 # the next daily run picks the version up anyway
}

data "aws_iam_policy_document" "scheduler_gtfs" {
  statement {
    actions   = ["lambda:InvokeFunction"]
    resources = [aws_lambda_function.gtfs.arn]
  }
}

resource "aws_iam_role_policy" "scheduler_gtfs" {
  name   = "invoke-gtfs"
  role   = aws_iam_role.scheduler.id
  policy = data.aws_iam_policy_document.scheduler_gtfs.json
}

resource "aws_scheduler_schedule" "gtfs" {
  name                         = "bahn-pulse-gtfs-daily"
  schedule_expression          = "cron(30 5 * * ? *)"
  schedule_expression_timezone = "UTC"
  state                        = var.poller_enabled ? "ENABLED" : "DISABLED"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_lambda_function.gtfs.arn
    role_arn = aws_iam_role.scheduler.arn
    input    = jsonencode({})

    retry_policy {
      maximum_retry_attempts = 0
    }
  }
}
