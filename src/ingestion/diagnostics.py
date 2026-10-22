"""Diagnostics redact exception chains and never print credential-bearing URLs."""
from src.validation.common import ValidationError

def run_diagnostic(main):
    try:return main()
    except Exception:
        raise ValidationError('Diagnostic API execution failed; endpoint details redacted') from None
