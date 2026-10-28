output "store_uri" {
  description = "agent.json \"store\"."
  value       = "s3://${local.bucket}/${trimsuffix(var.prefix, "/")}?region=${data.aws_region.current.name}"
}

output "agent_role_arn" {
  description = "The hcp-agent ServiceAccount's eks.amazonaws.com/role-arn."
  value       = aws_iam_role.this["agent"].arn
}

output "runner_role_arn" {
  description = "The hcp-runner ServiceAccount's eks.amazonaws.com/role-arn."
  value       = aws_iam_role.this["runner"].arn
}
