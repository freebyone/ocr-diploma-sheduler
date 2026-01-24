CREATE DATABASE ocr_db;
CREATE DATABASE norma_db;

\c ocr_db;

CREATE USER ocr_user WITH PASSWORD 'ocr_password';
GRANT ALL PRIVILEGES ON DATABASE ocr_db TO ocr_user;
ALTER DATABASE ocr_db OWNER TO ocr_user;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

\c norma_db;

CREATE USER norma_user WITH PASSWORD 'norma_password';
GRANT ALL PRIVILEGES ON DATABASE norma_db TO norma_user;
ALTER DATABASE norma_db OWNER TO norma_user;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

SELECT 
    'Database ocr_db created' as info,
    'User: ocr_user' as user_info,
    'Password: ocr_password' as password_info
UNION ALL
SELECT 
    'Database norma_db created',
    'User: norma_user',
    'Password: norma_password';