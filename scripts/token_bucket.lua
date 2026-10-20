-- Token Bucket: повертає 1, якщо жетон видано, і 0, якщо відро порожнє

-- 1. Вхідні дані
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])

-- 2. Поточний час (годинник Redis, один для всіх воркерів)
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000

-- 3. Що було минулого разу (якщо відра немає, воно повне)
local data = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(data[1]) or capacity
local ts = tonumber(data[2]) or now

-- 4. Скільки жетонів наросло, але не більше ємності
local elapsed = now - ts
tokens = math.min(capacity, tokens + elapsed * rate)

-- 5. Пробуємо взяти жетон
local allowed = 0
if tokens >= 1 then
    tokens = tokens - 1
    allowed = 1
end

-- 6. Зберегти і відповісти
redis.call('HSET', key, 'tokens', tokens, 'ts', now)
redis.call('EXPIRE', key, 60)
return allowed
