# OpenBoards poller: one Lambda function per tier, started by EventBridge Scheduler.
# Build the package first: scripts/build_lambda.sh (writes build/lambda/).

locals {
  poller_tiers = {
    "1" = { schedule = "rate(2 minutes)", rate = 4.5, timeout = 110 }
    "2" = { schedule = "rate(10 minutes)", rate = 1.5, timeout = 540 }
    "3" = { schedule = "rate(1 hour)", rate = 2.5, timeout = 890 }
  }
  # Sum of rates must stay below the OpenBoards limit of 10 requests per second.
  poller_total_rate = sum([for t in local.poller_tiers : t.rate])

  db_api_params = {
    client_id = "/bahn-pulse/db-api/client-id"
    api_key   = "/bahn-pulse/db-api/api-key"
  }
}

data "aws_caller_identity" "current" {}

data "archive_file" "poller" {
  type        = "zip"
  source_dir  = "${path.module}/../../../build/lambda"
  output_path = "${path.module}/../../../build/poller.zip"
}

# --- IAM: function role ---------------------------------------------------

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "poller" {
  name               = "bahn-pulse-openboards-poller"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

data "aws_iam_policy_document" "poller" {
  statement {
    sid       = "WriteBronze"
    actions   = ["s3:PutObject"]
    resources = ["${module.bronze_bucket.arn}/openboards/*"]
  }

  statement {
    sid     = "ReadDbApiKeys"
    actions = ["ssm:GetParameters"]
    resources = [
      for name in values(local.db_api_params) :
      "arn:aws:ssm:${var.region}:${data.aws_caller_identity.current.account_id}:parameter${name}"
    ]
  }

  statement {
    sid       = "WriteLogs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = [for lg in aws_cloudwatch_log_group.poller : "${lg.arn}:*"]
  }
}

resource "aws_iam_role_policy" "poller" {
  name   = "poller"
  role   = aws_iam_role.poller.id
  policy = data.aws_iam_policy_document.poller.json
}

# --- Lambda functions -----------------------------------------------------

resource "aws_cloudwatch_log_group" "poller" {
  for_each          = local.poller_tiers
  name              = "/aws/lambda/bahn-pulse-openboards-tier${each.key}"
  retention_in_days = 14
}

resource "aws_lambda_function" "poller" {
  for_each = local.poller_tiers

  function_name    = "bahn-pulse-openboards-tier${each.key}"
  description      = "Polls OpenBoards departures/arrivals for tier ${each.key} stations into bronze"
  role             = aws_iam_role.poller.arn
  runtime          = "python3.12"
  architectures    = ["arm64"]
  handler          = "bahn_pulse.openboards.lambda_handler.handler"
  filename         = data.archive_file.poller.output_path
  source_code_hash = data.archive_file.poller.output_base64sha256
  memory_size      = 256                # I/O-bound; 256 MB keeps compute near the free tier
  timeout          = each.value.timeout # below the schedule interval, so runs never overlap

  environment {
    variables = {
      BRONZE_BUCKET      = module.bronze_bucket.id
      OPENBOARDS_RATE    = tostring(each.value.rate)
      DB_CLIENT_ID_PARAM = local.db_api_params.client_id
      DB_API_KEY_PARAM   = local.db_api_params.api_key
      STATIONS_CSV       = "/var/task/config/station_tiers.csv"
    }
  }

  depends_on = [aws_cloudwatch_log_group.poller, aws_iam_role_policy.poller]

  lifecycle {
    precondition {
      condition     = local.poller_total_rate < 10
      error_message = "Sum of tier rates must stay below the OpenBoards limit of 10 req/s."
    }
  }
}

# --- Schedules --------------------------------------------------------------

data "aws_iam_policy_document" "scheduler_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
}

resource "aws_iam_role" "scheduler" {
  name               = "bahn-pulse-poller-scheduler"
  assume_role_policy = data.aws_iam_policy_document.scheduler_assume.json
}

data "aws_iam_policy_document" "scheduler" {
  statement {
    actions   = ["lambda:InvokeFunction"]
    resources = [for fn in aws_lambda_function.poller : fn.arn]
  }
}

resource "aws_iam_role_policy" "scheduler" {
  name   = "invoke-poller"
  role   = aws_iam_role.scheduler.id
  policy = data.aws_iam_policy_document.scheduler.json
}

resource "aws_scheduler_schedule" "poller" {
  for_each = local.poller_tiers

  name                = "bahn-pulse-openboards-tier${each.key}"
  schedule_expression = each.value.schedule
  state               = var.poller_enabled ? "ENABLED" : "DISABLED"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_lambda_function.poller[each.key].arn
    role_arn = aws_iam_role.scheduler.arn
    input    = jsonencode({ tier = tonumber(each.key) })

    retry_policy {
      maximum_retry_attempts = 0 # the next scheduled run comes soon anyway
    }
  }
}
