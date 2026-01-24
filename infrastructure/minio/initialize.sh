#!/bin/sh
set -e

echo "Жду запуска MinIO..."
sleep 5

echo "Настройка MinIO Client"
mc alias set localminio http://minio:9000 ocr-minio admin

mc mb localminio/documents --ignore-existing
mc mb localminio/templates --ignore-existing
mc mb localminio/exports --ignore-existing
mc mb localminio/uploads --ignore-existing
mc mb localminio/backups --ignore-existing

mc anonymous set download localminio/documents
mc anonymous set none localminio/templates
mc anonymous set none localminio/exports
mc anonymous set none localminio/uploads
mc anonymous set none localminio/backups

echo "Создаю пользователя для приложения..."
mc admin user add localminio appuser apppassword123

cat > /tmp/readwrite.json << EOF
{
  "Version": "2026-01-20",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:*"
      ],
      "Resource": [
        "arn:aws:s3:::*"
      ]
    }
  ]
}
EOF

mc admin policy create localminio readwrite /tmp/readwrite.json
mc admin policy attach localminio readwrite --user=appuser

echo "Инициализация MinIO завершена!"
echo ""
echo "Доступ:"
echo "Console: http://localhost:9001"
echo "API: http://localhost:9000"
echo "Root: ocr-minio / admin"
echo "App: appuser / apppassword123"