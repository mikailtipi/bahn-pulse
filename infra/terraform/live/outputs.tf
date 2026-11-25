output "bronze_bucket" {
  description = "Name of the bronze bucket."
  value       = module.bronze_bucket.id
}

output "poller_functions" {
  description = "Poller Lambda function names per tier."
  value       = { for tier, fn in aws_lambda_function.poller : tier => fn.function_name }
}

output "gtfs_function" {
  description = "GTFS loader Lambda function name."
  value       = aws_lambda_function.gtfs.function_name
}

output "openmeteo_function" {
  description = "Open-Meteo weather loader Lambda function name."
  value       = aws_lambda_function.openmeteo.function_name
}

output "dwd_warnings_function" {
  description = "DWD weather warnings loader Lambda function name."
  value       = aws_lambda_function.dwd_warnings.function_name
}

output "holidays_function" {
  description = "Holidays loader Lambda function name."
  value       = aws_lambda_function.holidays.function_name
}
