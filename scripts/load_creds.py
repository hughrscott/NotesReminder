#!/usr/bin/env python3
"""Export credentials from .sorenv for shell sourcing."""
import os, shlex

env = {}
sorenv_path = os.path.join(os.path.expanduser('~'), '.hermes', 'SOR', '.sorenv')
with open(sorenv_path) as f:
    for line in f:
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        if '=' in line:
            k, v = line.split('=', 1)
            v = v.strip().strip('"').strip("'")
            env[k] = v

# Also grab SOR_APP_PASSWORD from main .env
hermes_env = os.path.join(os.path.expanduser('~'), '.hermes', '.env')
with open(hermes_env) as f:
    for line in f:
        line = line.strip()
        if line.startswith('SOR_APP_PASSWORD='):
            env['SOR_APP_PASSWORD'] = line.split('=', 1)[1].strip()
            break

for k, v in env.items():
    print(f'export {k}={shlex.quote(v)}')
