# Holidays loader: weekly snapshot of German school and public holidays (OpenHolidays API).
# Same package as the poller (scripts/build_lambda.sh).

resource "aws_iam_role" "holidays" {
  name               = "bahn-pulse-holidays-loader"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

data "aws_iam_policy_document" "holidays" {
  statement {
    sid       = "WriteBronzeHolidays"
    actions   = ["s3:PutObject"]
    resources = ["${module.bronze_bucket.arn}/openholidays/*"]
  }

  statement {
    sid       = "WriteLogs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.holidays.arn}:*"]
  }
}

resource "aws_iam_role_policy" "holidays" {
  name   = "holidays-loader"
  role   = aws_iam_role.holidays.id
  policy = data.aws_iam_policy_document.holidays.json
}

resource "aws_cloudwatch_log_group" "holidays" {
  name              = "/aws/lambda/bahn-pulse-holidays"
  retention_in_days = 14
}

resource "aws_lambda_function" "holidays" {
  function_name    = "bahn-pulse-holidays"
  description      = "Weekly snapshot of German school and public holidays (OpenHolidays API) into bronze"
  role             = aws_iam_role.holidays.arn
  runtime          = "python3.12"
  architectures    = ["arm64"]
  handler          = "bahn_pulse.ingestion.holidays.handler"
  filename         = data.archive_file.poller.output_path
  source_code_hash = data.archive_file.poller.output_base64sha256
  memory_size      = 256
  timeout          = 60

  environment {
    variables = {
      BRONZE_BUCKET = module.bronze_bucket.id
    }
  }

  depends_on = [aws_cloudwatch_log_group.holidays, aws_iam_role_policy.holidays]
}

resource "aws_lambda_function_event_invoke_config" "holidays" {
  function_name          = aws_lambda_function.holidays.function_name
  maximum_retry_attempts = 0 # next week's run takes a new snapshot anyway
}

data "aws_iam_policy_document" "scheduler_holidays" {
  statement {
    actions   = ["lambda:InvokeFunction"]
    resources = [aws_lambda_function.holidays.arn]
  }
}

resource "aws_iam_role_policy" "scheduler_holidays" {
  name   = "invoke-holidays"
  role   = aws_iam_role.scheduler.id
  policy = data.aws_iam_policy_document.scheduler_holidays.json
}

resource "aws_scheduler_schedule" "holidays" {
  name                         = "bahn-pulse-holidays-weekly"
  schedule_expression          = "cron(0 4 ? * MON *)"
  schedule_expression_timezone = "UTC"
  state                        = var.poller_enabled ? "ENABLED" : "DISABLED"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_lambda_function.holidays.arn
    role_arn = aws_iam_role.scheduler.arn
    input    = jsonencode({})

    retry_policy {
      maximum_retry_attempts = 0
    }
  }
}
