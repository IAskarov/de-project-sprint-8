-- Входная таблица: подписки пользователей на рестораны (избранное)
CREATE TABLE IF NOT EXISTS public.subscribers_restaurants (
    id serial4 NOT NULL,
    client_id varchar NOT NULL,
    restaurant_id varchar NOT NULL,
    CONSTRAINT pk_id PRIMARY KEY (id)
);

INSERT INTO public.subscribers_restaurants (client_id, restaurant_id)
SELECT v.client_id, v.restaurant_id
FROM (VALUES
  ('223e4567-e89b-12d3-a456-426614174000', '123e4567-e89b-12d3-a456-426614174000'),
  ('323e4567-e89b-12d3-a456-426614174000', '123e4567-e89b-12d3-a456-426614174000'),
  ('423e4567-e89b-12d3-a456-426614174000', '123e4567-e89b-12d3-a456-426614174000'),
  ('523e4567-e89b-12d3-a456-426614174000', '123e4567-e89b-12d3-a456-426614174000'),
  ('623e4567-e89b-12d3-a456-426614174000', '123e4567-e89b-12d3-a456-426614174000'),
  ('723e4567-e89b-12d3-a456-426614174000', '123e4567-e89b-12d3-a456-426614174000'),
  ('823e4567-e89b-12d3-a456-426614174000', '123e4567-e89b-12d3-a456-426614174000'),
  ('923e4567-e89b-12d3-a456-426614174000', '123e4567-e89b-12d3-a456-426614174001'),
  ('023e4567-e89b-12d3-a456-426614174000', '123e4567-e89b-12d3-a456-426614174000'),
  ('123e4567-e89b-12d3-a456-426614174000', '123e4567-e89b-12d3-a456-426614174000')
) AS v(client_id, restaurant_id)
WHERE NOT EXISTS (
  SELECT 1 FROM public.subscribers_restaurants s
  WHERE s.client_id = v.client_id AND s.restaurant_id = v.restaurant_id
);
