# Open-Meteo weather loader: once a day, loads the previous UTC day for all station grid cells
# (~3,800 calls, ~9 minutes). Uses the same package as the poller (scripts/build_lambda.sh).

resource "aws_iam_role" "openmeteo" {
  name               = "bahn-pulse-openmeteo-loader"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

data "aws_iam_policy_document" "openmeteo" {
  statement {
    sid       = "WriteBronzeWeather"
    actions   = ["s3:PutObject"]
    resources = ["${module.bronze_bucket.arn}/openmeteo/*"]
  }

  statement {
    sid       = "ListBronzeWeather" # is this day already loaded?
    actions   = ["s3:ListBucket"]
    resources = [module.bronze_bucket.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["openmeteo/*"]
    }
  }

  statement {
    sid       = "WriteLogs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.openmeteo.arn}:*"]
  }
}

resource "aws_iam_role_policy" "openmeteo" {
  name   = "openmeteo-loader"
  role   = aws_iam_role.openmeteo.id
  policy = data.aws_iam_policy_document.openmeteo.json
}

resource "aws_cloudwatch_log_group" "openmeteo" {
  name              = "/aws/lambda/bahn-pulse-openmeteo"
  retention_in_days = 14
}

resource "aws_lambda_function" "openmeteo" {
  function_name    = "bahn-pulse-openmeteo"
  description      = "Loads yesterday's hourly Open-Meteo weather for all station grid cells into bronze"
  role             = aws_iam_role.openmeteo.arn
  runtime          = "python3.12"
  architectures    = ["arm64"]
  handler          = "bahn_pulse.ingestion.openmeteo.handler"
  filename         = data.archive_file.poller.output_path
  source_code_hash = data.archive_file.poller.output_base64sha256
  memory_size      = 256
  timeout          = 900 # a full day takes ~9 minutes (paced to the API's per-minute limit)

  environment {
    variables = {
      BRONZE_BUCKET = module.bronze_bucket.id
      STATIONS_CSV  = "/var/task/config/station_tiers.csv"
    }
  }

  depends_on = [aws_cloudwatch_log_group.openmeteo, aws_iam_role_policy.openmeteo]
}

resource "aws_lambda_function_event_invoke_config" "openmeteo" {
  function_name          = aws_lambda_function.openmeteo.function_name
  maximum_retry_attempts = 0 # a failed day can be loaded again by hand ({"date": "YYYY-MM-DD"})
}

data "aws_iam_policy_document" "scheduler_openmeteo" {
  statement {
    actions   = ["lambda:InvokeFunction"]
    resources = [aws_lambda_function.openmeteo.arn]
  }
}

resource "aws_iam_role_policy" "scheduler_openmeteo" {
  name   = "invoke-openmeteo"
  role   = aws_iam_role.scheduler.id
  policy = data.aws_iam_policy_document.scheduler_openmeteo.json
}

resource "aws_scheduler_schedule" "openmeteo" {
  name                         = "bahn-pulse-openmeteo-daily"
  schedule_expression          = "cron(30 2 * * ? *)"
  schedule_expression_timezone = "UTC"
  state                        = var.poller_enabled ? "ENABLED" : "DISABLED"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_lambda_function.openmeteo.arn
    role_arn = aws_iam_role.scheduler.arn
    input    = jsonencode({})

    retry_policy {
      maximum_retry_attempts = 0
    }
  }
}
