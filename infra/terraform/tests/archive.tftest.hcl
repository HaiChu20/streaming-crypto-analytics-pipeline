# Every run uses the mocked provider and only plans; no AWS credentials are needed.
mock_provider "aws" {}

variables {
  bucket_name  = "p14-archive-test"
  region       = "eu-north-1"
  budget_email = "archive-test@example.com"
}

# The policy needs this computed ARN during plan, before a mock apply could fill it.
override_resource {
  target          = aws_s3_bucket.archive
  override_during = plan
  values = {
    arn = "arn:aws:s3:::p14-archive-test"
  }
}

run "archive_defaults" {
  command = plan

  assert {
    condition = (
      aws_s3_bucket_public_access_block.archive.block_public_acls &&
      aws_s3_bucket_public_access_block.archive.block_public_policy &&
      aws_s3_bucket_public_access_block.archive.ignore_public_acls &&
      aws_s3_bucket_public_access_block.archive.restrict_public_buckets
    )
    error_message = "All four S3 public access protections must be enabled."
  }

  assert {
    condition     = one(aws_s3_bucket_server_side_encryption_configuration.archive.rule).apply_server_side_encryption_by_default[0].sse_algorithm == "AES256"
    error_message = "Archive objects must use AES256 server-side encryption by default."
  }

  assert {
    condition     = one(aws_s3_bucket_ownership_controls.archive.rule).object_ownership == "BucketOwnerEnforced"
    error_message = "The bucket owner must own all archived objects."
  }

  assert {
    condition = (
      one(aws_s3_bucket_lifecycle_configuration.archive.rule).status == "Enabled" &&
      one(one(aws_s3_bucket_lifecycle_configuration.archive.rule).abort_incomplete_multipart_upload).days_after_initiation == 7
    )
    error_message = "Incomplete multipart uploads must be aborted after seven days."
  }

  assert {
    condition     = aws_iam_user.archiver.name == "${var.bucket_name}-archiver"
    error_message = "The archiver IAM user must be named after the bucket."
  }

  assert {
    condition = jsondecode(aws_iam_user_policy.archiver.policy) == {
      Version = "2012-10-17"
      Statement = [
        {
          Effect   = "Allow"
          Action   = ["s3:ListBucket"]
          Resource = "arn:aws:s3:::${var.bucket_name}"
        },
        {
          Effect   = "Allow"
          Action   = ["s3:PutObject", "s3:GetObject"]
          Resource = "arn:aws:s3:::${var.bucket_name}/*"
        }
      ]
    }
    error_message = "The inline policy must grant exactly ListBucket on this bucket and PutObject/GetObject on its objects."
  }

  assert {
    condition = (
      aws_budgets_budget.archive.limit_amount == "10" &&
      aws_budgets_budget.archive.limit_unit == "USD" &&
      aws_budgets_budget.archive.time_unit == "MONTHLY"
    )
    error_message = "The default monthly budget must be 10 USD."
  }

  assert {
    condition = (
      length(aws_budgets_budget.archive.notification) == 2 &&
      alltrue([
        for notification in aws_budgets_budget.archive.notification :
        notification.subscriber_email_addresses == toset([var.budget_email])
      ])
    )
    error_message = "Both budget notifications must go only to the supplied email address."
  }
}

run "custom_budget" {
  command = plan

  variables {
    budget_usd = 25
  }

  assert {
    condition = (
      aws_budgets_budget.archive.limit_amount == "25" &&
      aws_budgets_budget.archive.limit_unit == "USD"
    )
    error_message = "Setting budget_usd to 25 must change the limit to 25 USD."
  }
}
