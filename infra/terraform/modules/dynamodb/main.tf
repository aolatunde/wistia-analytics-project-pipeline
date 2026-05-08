resource "aws_dynamodb_table" "watermarks" {
  name         = var.table_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "dataset"

  attribute {
    name = "dataset"
    type = "S"
  }

  point_in_time_recovery { enabled = true }

  tags = var.tags
}

resource "aws_dynamodb_table_item" "media_metadata_seed" {
  table_name = aws_dynamodb_table.watermarks.name
  hash_key   = aws_dynamodb_table.watermarks.hash_key

  item = jsonencode({
    dataset                = { S = "media_metadata" }
    last_success_watermark = { S = "1970-01-01T00:00:00Z" }
    last_run_status        = { S = "SEEDED" }
  })
}

resource "aws_dynamodb_table_item" "media_engagement_seed" {
  table_name = aws_dynamodb_table.watermarks.name
  hash_key   = aws_dynamodb_table.watermarks.hash_key

  item = jsonencode({
    dataset                = { S = "media_engagement" }
    last_success_watermark = { S = "1970-01-01T00:00:00Z" }
    last_run_status        = { S = "SEEDED" }
  })
}
