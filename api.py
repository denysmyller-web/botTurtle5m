from flask import Blueprint, redirect, render_template, request, send_file, session, url_for
from security import allow_default_local_access, credentials, login_required, password_matches

api = Blueprint('api', __name__)


@api.route('/login', methods=['GET', 'POST'])
def login():
    error = None
    username_configured, _, _ = credentials()
    if allow_default_local_access():
        session.clear()
        session['authenticated'] = True
        session.permanent = True
        return redirect(url_for('dashboard'))
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '')
        if username_configured and username == username_configured and password_matches(password):
            session.clear()
            session['authenticated'] = True
            session.permanent = True
            return redirect(url_for('dashboard'))
        error = 'Usuário ou senha inválidos.'
    return render_template('login.html', error=error)


@api.post('/logout')
def logout():
    session.clear()
    return redirect(url_for('api.login'))


@api.post('/api/start')
@login_required
def start():
    import app
    started = app.start_bot()
    if not started and app.last_bot_error:
        return {'ok': False, 'started': False, 'running': app.is_bot_running(), 'error': app.last_bot_error}, 400
    return {'ok': True, 'started': started, 'running': app.is_bot_running()}


@api.post('/api/stop')
@login_required
def stop():
    from app import is_bot_running, stop_bot
    stopped = stop_bot()
    return {'ok': True, 'stopped': stopped, 'running': is_bot_running()}


@api.post('/api/tradingview/webhook')
def tradingview_webhook():
    import app
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return {'ok': False, 'error': 'Envie um objeto JSON.'}, 400
    queued, status_code, message = app.queue_tradingview_alert(data)
    result = {'ok': queued, 'message' if queued else 'error': message}
    return result, status_code


@api.post('/api/settings')
@login_required
def settings():
    import app
    data = request.get_json(silent=True) or {}
    try:
        leverage = int(data.get('leverage', app.LEVERAGE))
        position_pct = float(data.get('position_pct', 100))
        if leverage < 1 or leverage > 10:
            raise ValueError('Alavancagem deve estar entre 1x e 10x.')
        if position_pct != 100:
            raise ValueError('O percentual por entrada está fixado em 100% do saldo disponível.')
        app.LEVERAGE = leverage
        app.POSITION_SIZE_PCT = 1.0
        app.ENTRY_SIZE_PCT = 1.0
        print(
            f'Configuração atualizada: alavancagem {leverage}x, '
            f'entrada com {position_pct:.2f}% do saldo disponível; '
            'uma posição por vez',
            flush=True,
        )
        return {'ok': True, 'leverage': leverage, 'position_pct': position_pct}
    except (TypeError, ValueError) as exc:
        return {'ok': False, 'error': str(exc)}, 400


@api.post('/api/change-password')
@login_required
def change_password():
    from werkzeug.security import generate_password_hash
    import os
    import app
    
    data = request.get_json(silent=True) or {}
    old_password = data.get('old_password', '')
    new_password = data.get('new_password', '')
    
    if not password_matches(old_password):
        return {'ok': False, 'error': 'Senha atual incorreta.'}, 403
        
    if len(new_password) < 6:
        return {'ok': False, 'error': 'A nova senha deve ter no mínimo 6 caracteres.'}, 400
        
    # Gera o novo hash padrão scrypt utilizado no ecossistema
    new_hash = generate_password_hash(new_password)
    
    env_path = os.path.join(app.BASE_DIR, 'env.txt')
    try:
        with open(env_path, 'r') as f:
            lines = f.readlines()
            
        with open(env_path, 'w') as f:
            for line in lines:
                if line.startswith('ADMIN_PASSWORD_HASH='):
                    f.write(f'ADMIN_PASSWORD_HASH={new_hash}\n')
                else:
                    f.write(line)
                    
        return {'ok': True, 'message': 'Senha alterada com sucesso! Na próxima sessão, use a nova senha.'}
    except Exception as e:
        return {'ok': False, 'error': f'Erro ao salvar no arquivo de ambiente: {str(e)}'}, 500


@api.get('/api/scan-status')
@login_required
def scan_status():
    import app
    if not app.is_bot_running():
        app.ensure_read_only_market_scan()
    with app.bot_state_lock:
        coins = sorted(
            app.scan_status.values(),
            key=lambda item: (
                item.get('status') == 'COMPRA',
                float(item.get('buy_analysis', {}).get('turnover_5m_usdt') or 0),
            ),
            reverse=True,
        )
    return {
        'ok': True,
        'running': app.is_bot_running(),
        'analysis_mode': 'BOT' if app.is_bot_running() else 'SOMENTE_LEITURA',
        'cycle_seconds': app.SCAN_INTERVAL_SECONDS,
        'last_error': app.last_bot_error,
        'coins': coins,
    }


@api.get('/api/trade-charts')
@login_required
def trade_charts():
    import app
    return {'ok': True, 'symbols': app.get_trade_chart_data()}


@api.get('/api/open-positions')
@login_required
def open_positions():
    import app
    positions, source = app.get_dashboard_open_positions()
    if source.startswith('BYBIT indisponível:'):
        return {'ok': False, 'positions': [], 'error': source}, 503
    return {'ok': True, 'positions': positions, 'source': source}


@api.get('/api/open-position-charts')
@login_required
def open_position_charts():
    import app
    charts, source = app.get_open_position_chart_data()
    if source.startswith('BYBIT indisponível:'):
        return {'ok': False, 'charts': [], 'error': source}, 503
    return {'ok': True, 'charts': charts, 'source': source}


@api.post('/api/scan-clear')
@login_required
def clear_scan_status():
    import app
    with app.bot_state_lock:
        cleared = len(app.scan_status)
        app.scan_status.clear()
    return {'ok': True, 'cleared': cleared}


@api.get('/api/analysis-log.csv')
@login_required
def download_analysis_log():
    import app
    app.ensure_analysis_log_file()
    return send_file(
        app.ANALYSIS_LOG_CSV,
        as_attachment=True,
        download_name='coin_analysis_log.csv',
        mimetype='text/csv',
    )


@api.get('/api/available-symbols')
@login_required
def available_symbols():
    import app
    symbols = app.get_all_usdt_pairs(limit_list=False)
    if not symbols:
        return {'ok': False, 'error': 'Não foi possível carregar os contratos da Bybit.'}, 503
    return {'ok': True, 'count': len(symbols), 'symbols': symbols}


@api.get('/api/wallet-balance')
@login_required
def wallet_balance():
    import app
    balance, source = app.get_dashboard_wallet_balance()
    return {
        'ok': balance is not None,
        'available_usdt': balance,
        'source': source,
        'cached': 'último valor em cache' in source,
        'error': app.wallet_balance_last_error,
    }, 200 if balance is not None else 503