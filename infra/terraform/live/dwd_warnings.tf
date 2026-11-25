# DWD weather warnings loader: every hour, copies new CAP packages from DWD open data.
# DWD keeps packages ~48 h, so a few missed runs lose nothing. Same package as the poller.

resource "aws_iam_role" "dwd_warnings" {
  name               = "bahn-pulse-dwd-warnings-loader"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

data "aws_iam_policy_document" "dwd_warnings" {
  statement {
    sid       = "WriteBronzeDwd"
    actions   = ["s3:PutObject"]
    resources = ["${module.bronze_bucket.arn}/dwd/*"]
  }

  statement {
    sid       = "ListBronzeDwd" # which packages are already copied?
    actions   = ["s3:ListBucket"]
    resources = [module.bronze_bucket.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["dwd/*"]
    }
  }

  statement {
    sid       = "WriteLogs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.dwd_warnings.arn}:*"]
  }
}

resource "aws_iam_role_policy" "dwd_warnings" {
  name   = "dwd-warnings-loader"
  role   = aws_iam_role.dwd_warnings.id
  policy = data.aws_iam_policy_document.dwd_warnings.json
}

resource "aws_cloudwatch_log_group" "dwd_warnings" {
  name              = "/aws/lambda/bahn-pulse-dwd-warnings"
  retention_in_days = 14
}

resource "aws_lambda_function" "dwd_warnings" {
  function_name    = "bahn-pulse-dwd-warnings"
  description      = "Copies new DWD CAP weather warning packages (municipality level) into bronze"
  role             = aws_iam_role.dwd_warnings.arn
  runtime          = "python3.12"
  architectures    = ["arm64"]
  handler          = "bahn_pulse.ingestion.dwd_warnings.handler"
  filename         = data.archive_file.poller.output_path
  source_code_hash = data.archive_file.poller.output_base64sha256
  memory_size      = 256
  timeout          = 300 # an hourly run copies ~6 small files; a 2-day catch-up takes ~2 min

  environment {
    variables = {
      BRONZE_BUCKET = module.bronze_bucket.id
    }
  }

  depends_on = [aws_cloudwatch_log_group.dwd_warnings, aws_iam_role_policy.dwd_warnings]
}

resource "aws_lambda_function_event_invoke_config" "dwd_warnings" {
  function_name          = aws_lambda_function.dwd_warnings.function_name
  maximum_retry_attempts = 0 # the next hourly run catches up
}

data "aws_iam_policy_document" "scheduler_dwd_warnings" {
  statement {
    actions   = ["lambda:InvokeFunction"]
    resources = [aws_lambda_function.dwd_warnings.arn]
  }
}

resource "aws_iam_role_policy" "scheduler_dwd_warnings" {
  name   = "invoke-dwd-warnings"
  role   = aws_iam_role.scheduler.id
  policy = data.aws_iam_policy_document.scheduler_dwd_warnings.json
}

resource "aws_scheduler_schedule" "dwd_warnings" {
  name                         = "bahn-pulse-dwd-warnings-hourly"
  schedule_expression          = "cron(12 * * * ? *)"
  schedule_expression_timezone = "UTC"
  state                        = var.poller_enabled ? "ENABLED" : "DISABLED"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_lambda_function.dwd_warnings.arn
    role_arn = aws_iam_role.scheduler.arn
    input    = jsonencode({})

    retry_policy {
      maximum_retry_attempts = 0
    }
  }
}
