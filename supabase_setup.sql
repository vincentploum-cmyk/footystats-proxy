-- Predictions table: stores every pre-match prediction
CREATE TABLE predictions (
  id BIGINT PRIMARY KEY,
  home_id INT,
  away_id INT,
  home TEXT,
  away TEXT,
  league TEXT,
  league_sid INT,
  match_date TEXT,
  kickoff TIMESTAMPTZ,
  ci NUMERIC(5,2),
  def_ci NUMERIC(5,2),
  rank INT,
  label TEXT,
  prob15 NUMERIC(5,1),
  prob25 NUMERIC(5,1),
  sig_a BOOLEAN,
  sig_b BOOLEAN,
  sig_c BOOLEAN,
  sig_d BOOLEAN,
  h_scored_fh NUMERIC(5,2),
  h_conced_fh NUMERIC(5,2),
  h_t1_pct NUMERIC(5,1),
  a_scored_fh NUMERIC(5,2),
  a_conced_fh NUMERIC(5,2),
  a_t1_pct NUMERIC(5,1),
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Outcomes table: stores actual match results
CREATE TABLE outcomes (
  id BIGINT PRIMARY KEY REFERENCES predictions(id),
  fh_home INT,
  fh_away INT,
  ft_home INT,
  ft_away INT,
  fh_total INT,
  hit_15 BOOLEAN,
  hit_25 BOOLEAN,
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Index for fast lookups
CREATE INDEX idx_predictions_date ON predictions(match_date);
CREATE INDEX idx_predictions_rank ON predictions(rank);
CREATE INDEX idx_predictions_league ON predictions(league_sid);
CREATE INDEX idx_outcomes_hit25 ON outcomes(hit_25);

-- Enable Row Level Security (required by Supabase)
ALTER TABLE predictions ENABLE ROW LEVEL SECURITY;
ALTER TABLE outcomes ENABLE ROW LEVEL SECURITY;

-- Allow anon key to read and write
CREATE POLICY "Allow all on predictions" ON predictions FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all on outcomes" ON outcomes FOR ALL USING (true) WITH CHECK (true);
