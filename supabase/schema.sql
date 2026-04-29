-- footystats-proxy Supabase schema
--
-- Apply manually via Supabase SQL editor (Project → SQL → New query → paste → Run).
-- Safe to re-run: all statements are idempotent.
--
-- Tables:
--   match_results       — one row per completed match with frozen pre-match prediction
--   league_prob_tables  — Phase 2 per-league recalibrated probability tables (not yet read at runtime)

create table if not exists match_results (
  match_id        bigint primary key,
  competition_id  integer,
  league_name     text,
  home_id         integer,
  away_id         integer,
  home_name       text,
  away_name       text,
  date_unix       bigint,
  ht_home         smallint,
  ht_away         smallint,
  ft_home         smallint,
  ft_away         smallint,
  fh_total        smallint,
  hit_15          boolean,
  hit_25          boolean,
  rank            smallint,
  ci              numeric(6,2),
  def_ci          numeric(6,2),
  prob25          numeric(6,2),
  prob15          numeric(6,2),
  signals         jsonb,
  snap            jsonb,
  recorded_at     timestamptz not null default now()
);

create index if not exists idx_match_results_competition on match_results(competition_id);
create index if not exists idx_match_results_rank        on match_results(rank);
create index if not exists idx_match_results_date_unix   on match_results(date_unix);

create table if not exists league_prob_tables (
  competition_id  integer not null,
  rank            smallint not null,
  n               integer not null,
  prob25          numeric(6,2) not null,
  prob15          numeric(6,2) not null,
  updated_at      timestamptz not null default now(),
  primary key (competition_id, rank)
);

-- Row Level Security
-- The proxy uses the anon key. Allow it to insert/select match_results and read league_prob_tables.
-- Recalibration jobs (Phase 2) should run with the service role key, which bypasses RLS.
alter table match_results       enable row level security;
alter table league_prob_tables  enable row level security;

drop policy if exists match_results_anon_rw on match_results;
create policy match_results_anon_rw on match_results
  for all
  to anon
  using (true)
  with check (true);

drop policy if exists league_prob_tables_anon_read on league_prob_tables;
create policy league_prob_tables_anon_read on league_prob_tables
  for select
  to anon
  using (true);
