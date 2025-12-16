# execution_core.py
import numpy as np
from numba import njit

@njit(fastmath=True)
def simulate_core_logic(
    opens, highs, lows, closes, atrs, day_ids,
    p_longs, p_shorts, regimes,
    sl_mult, tp_mult, conf_threshold, vol_exit_mult,
    trail_on, trail_act_mult, trail_off_mult, 
    max_hold_bars,
    pullback_mult, fill_wait_bars, abort_threshold,
    mode_sniper, commission, deposit, risk_per_trade,
    whale_footprints,
    iceberg_pressures
):
    n = len(closes)
    equity = np.zeros(n)
    
    in_position = False; pos_type = 0; entry_price = 0.0; entry_idx = 0; pos_size = 0.0   
    sl_price = 0.0; tp_price = 0.0
    
    # Moon Mode State
    is_moon_active = False 
    
    pending_type = 0; pending_price = 0.0; pending_sl_dist = 0.0; pending_tp_dist = 0.0; pending_start_idx = 0
    current_balance = deposit
    out_trades = np.zeros((10000, 7), dtype=np.float64); t_ptr = 0
    
    for i in range(1, n):
        equity[i] = current_balance
        op = opens[i]; hi = highs[i]; lo = lows[i]; cl = closes[i]; atr = atrs[i]

        # --- 1. PENDING ORDER EXPIRATION (FIXED BUG) ---
        # Если ордер висит слишком долго — отменяем его
        if pending_type != 0:
            if (i - pending_start_idx) > fill_wait_bars:
                pending_type = 0  # Сброс зомби-ордера
                # Мы не делаем continue, чтобы дать шанс найти новый сигнал прямо на этом баре
        
        # --- 2. PENDING ORDER FILL LOGIC ---
        is_filled = False
        if pending_type == 1:
            if lo <= pending_price:
                in_position = True
                pos_type = 1
                entry_price = pending_price
                if op < pending_price: entry_price = op  # Gap protection

                sl_price = entry_price - pending_sl_dist
                tp_price = entry_price + pending_tp_dist
                entry_idx = i

                risk_amt = current_balance * risk_per_trade
                dist_to_sl = pending_sl_dist if pending_sl_dist > 0.0 else atr

                if dist_to_sl <= 0.0:
                    in_position = False; pos_type = 0; is_filled = False
                else:
                    pos_size = risk_amt / dist_to_sl
                    current_balance -= (pos_size * entry_price * commission)
                    is_filled = True

        elif pending_type == -1:
            if hi >= pending_price:
                in_position = True
                pos_type = -1
                entry_price = pending_price
                if op > pending_price: entry_price = op  # Gap protection

                sl_price = entry_price + pending_sl_dist
                tp_price = entry_price - pending_tp_dist
                entry_idx = i

                risk_amt = current_balance * risk_per_trade
                dist_to_sl = pending_sl_dist if pending_sl_dist > 0.0 else atr

                if dist_to_sl <= 0.0:
                    in_position = False; pos_type = 0; is_filled = False
                else:
                    pos_size = risk_amt / dist_to_sl
                    current_balance -= (pos_size * entry_price * commission)
                    is_filled = True

        if is_filled:
            pending_type = 0
            is_moon_active = False
            continue

        # --- POSITION MANAGEMENT ---
        if in_position:
            exit_signal = False; exit_price = 0.0; reason = 0 
            
            # --- [1. WHALE FOOTPRINT DETECTOR 🐋] ---
            # Новая фича из features_lib: whale_footprint и iceberg_pressure
            # Считываем индикатор "следа кита" на текущем баре
            whale_signal = 0
            iceberg_val = 0.0
            
            # Проверяем, есть ли в DataFrame нужные колонки (если фичи включены)
            # Внимание: execution_core работает с numpy-массивами, 
            # поэтому нужно передать whale_footprint как параметр функции
            # ИЛИ считать его здесь на лету (второй вариант ниже)
            
            # Если whale_footprint уже есть в массиве (добавь параметр в функцию)
            whale_signal = whale_footprints[i]  # 0 или 1
            iceberg_val = iceberg_pressures[i]   # float
            
            # Берем объем из... стоп, у нас нет volume в ядре!
            # Значит, используем косвенный индикатор: если бар ОЧЕНЬ маленький (< 0.3 ATR)
            # при этом цена НЕ двигается (abs(cl - op) < 0.2 ATR), но мы ВНУТРИ позиции —
            # это может быть признак накопления/распределения
            
            # --- [2. MOON MODE DETECTOR & ADAPTIVE TARGETS] ---
            # Считаем дистанцию от входа в ATR
            dist_from_entry_val = 0.0
            if pos_type == 1: 
                dist_from_entry_val = cl - entry_price
            else: 
                dist_from_entry_val = entry_price - cl
            
            atr_dist = 0.0
            if atr > 0.000001: 
                atr_dist = dist_from_entry_val / atr
            
            # Активация режима "РАКЕТА" 🚀
            # Триггеры:
            # 1. Цена улетела > 4 ATR от входа (классический брейкаут)
            # 2. ИЛИ обнаружен "след кита" при прибыли > 2 ATR (накопление перед импульсом)
            rocket_distance_trigger = (atr_dist > 4.0)
            whale_boost_trigger = (whale_signal > 0 and atr_dist > 2.0)
            
            if rocket_distance_trigger or whale_boost_trigger:
                is_moon_active = True
            
            # Если Луна активна — отодвигаем TP в космос
            current_tp_target = tp_price
            if is_moon_active:
                if pos_type == 1: 
                    current_tp_target = entry_price + (atr * 100.0)
                else: 
                    current_tp_target = entry_price - (atr * 100.0)
            
            # --- [3. CHECK HARD SL/TP] ---
            if pos_type == 1:
                # Long: проверяем пробой стопа вниз или тейка вверх
                if lo <= sl_price: 
                    exit_signal = True; exit_price = sl_price; reason = 0
                    if op < sl_price: exit_price = op  # Gap protection
                elif hi >= current_tp_target:
                    exit_signal = True; exit_price = current_tp_target; reason = 1
                    if op > current_tp_target: exit_price = op
            else:
                # Short: зеркально
                if hi >= sl_price:
                    exit_signal = True; exit_price = sl_price; reason = 0
                    if op > sl_price: exit_price = op
                elif lo <= current_tp_target:
                    exit_signal = True; exit_price = current_tp_target; reason = 1
                    if op < current_tp_target: exit_price = op
            
            # --- [4. DYNAMIC TRAILING STOP (3-РЕЖИМНЫЙ)] ---
            if not exit_signal and trail_on > 0.5:
                
                # Выбираем ширину трейлинга в зависимости от фазы сделки:
                # 
                # PHASE 1: START (0-1.5 ATR) — Узкий стоп для защиты капитала
                # PHASE 2: TREND (1.5-4 ATR) — Средний стоп, даем тренду дышать
                # PHASE 3: ROCKET (>4 ATR) — Широкий стоп, ловим "хвост ракеты"
                
                current_trail_mult = trail_off_mult  # Дефолт из конфига (обычно 1.2-1.5)
                
                if is_moon_active:
                    # РЕЖИМ РАКЕТЫ 🚀: Максимальная свобода (3.5 ATR от Close)
                    current_trail_mult = 3.5
                    
                    # БОНУС: Если обнаружен "след кита" — ещё шире (кит копит на новый импульс)
                    if whale_signal > 0:
                        current_trail_mult = 4.5  # Даем киту докупиться
                        
                elif atr_dist > 1.5:
                    # ХОРОШИЙ ТРЕНД: Чуть шире стандарта (1.8 ATR)
                    current_trail_mult = 1.8
                else:
                    # НАЧАЛО СДЕЛКИ: Короткий стоп (1.0 ATR) для минимизации риска
                    current_trail_mult = 1.0
                
                # Применяем трейлинг (только улучшаем цену стопа)
                if pos_type == 1:
                    new_sl = cl - (atr * current_trail_mult)
                    if new_sl > sl_price: 
                        sl_price = new_sl
                elif pos_type == -1:
                    new_sl = cl + (atr * current_trail_mult)
                    if new_sl < sl_price: 
                        sl_price = new_sl
            
            # --- [END OF WHALE + MOON LOGIC] ---

            # 3. Time Exit & 4. Smart Cut & 5. Volatility Panic Exit
            if not exit_signal and (i - entry_idx) > max_hold_bars:
                exit_signal = True; exit_price = cl; reason = 3
                
            abort_threshold_dynamic = abort_threshold
            if is_moon_active: abort_threshold_dynamic = 0.98 # В ракете терпим почти всё

            if not exit_signal:
                p_l_curr = p_longs[i]; p_s_curr = p_shorts[i]
                if pos_type == 1 and p_s_curr > abort_threshold_dynamic:
                    exit_signal = True; exit_price = cl; reason = 4
                elif pos_type == -1 and p_l_curr > abort_threshold_dynamic:
                    exit_signal = True; exit_price = cl; reason = 4
                    
            if not exit_signal:
                bar_size = hi - lo
                if bar_size > (atr * vol_exit_mult):
                    if pos_type == 1 and cl < op: exit_signal = True; exit_price = cl; reason = 2
                    elif pos_type == -1 and cl > op: exit_signal = True; exit_price = cl; reason = 2

            if exit_signal:
                pnl = 0.0
                if pos_type == 1: pnl = (exit_price - entry_price) / entry_price
                else: pnl = (entry_price - exit_price) / entry_price
                
                current_balance -= (pos_size * exit_price * commission)
                profit_abs = pos_size * (exit_price - entry_price) if pos_type == 1 else pos_size * (entry_price - exit_price)
                current_balance += profit_abs
                
                if t_ptr < 10000:
                    out_trades[t_ptr, 0] = entry_idx; out_trades[t_ptr, 1] = i
                    out_trades[t_ptr, 2] = entry_price; out_trades[t_ptr, 3] = exit_price
                    out_trades[t_ptr, 4] = pos_type; out_trades[t_ptr, 5] = pnl
                    final_reason = reason
                    if is_moon_active and reason == 0: final_reason = 5 
                    out_trades[t_ptr, 6] = final_reason
                    t_ptr += 1
                in_position = False; pos_type = 0; pending_type = 0; is_moon_active = False; continue 

        # --- ENTRY LOGIC ---
        if not in_position and pending_type == 0:
            p_long = p_longs[i]; p_short = p_shorts[i]
            valid_signal = False; new_type = 0
            
            if p_long > conf_threshold: new_type = 1; valid_signal = True
            elif p_short > conf_threshold: new_type = -1; valid_signal = True
                
            if valid_signal:
                pullback_dist = atr * pullback_mult
                if new_type == 1:
                    pending_price = cl - pullback_dist 
                    if pending_price > hi: pending_price = cl 
                else: 
                    pending_price = cl + pullback_dist 
                    if pending_price < lo: pending_price = cl
                
                pending_type = new_type; pending_start_idx = i
                pending_sl_dist = atr * sl_mult; pending_tp_dist = atr * tp_mult

    return equity, out_trades[:t_ptr]