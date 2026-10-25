output "bucket_name" {
  value = aws_s3_bucket.archive.id
}

output "archiver_user_name" {
  value = aws_iam_user.archiver.name
}
