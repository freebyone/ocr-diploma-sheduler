#!/bin/sh
set -e

mc alias set localminio http://minio:9000 ocrminio admin123456

mc mb localminio/documents --ignore-existing
mc mb localminio/templates --ignore-existing

mc anonymous set download localminio/documents
mc anonymous set none localminio/templates

mc admin user add localminio appuser apppassword123

cat > /tmp/readwrite.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::documents/*",
        "arn:aws:s3:::templates/*"
      ]
    }
  ]
}
EOF

mc admin policy create localminio readwrite /tmp/readwrite.json
mc admin policy attach localminio readwrite --user=appuser

echo "Console: http://localhost:9001"
echo "API: http://localhost:9000"
echo "Root: ocrminio / admin123456"
echo "App: appuser / apppassword123"