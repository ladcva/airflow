python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
python -c 'import secrets; print(secrets.token_urlsafe(32))'