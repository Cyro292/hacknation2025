-- Run this SQL in your Supabase SQL Editor to create the markets table

CREATE TABLE IF NOT EXISTS markets (
    id BIGSERIAL PRIMARY KEY,
    question TEXT,
    description TEXT,
    outcomes TEXT[],
    outcome_prices TEXT[],
    end_date TIMESTAMPTZ,
    volume NUMERIC,
    is_active BOOLEAN,
    polymarket_id TEXT UNIQUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_markets_is_active ON markets(is_active);
CREATE INDEX IF NOT EXISTS idx_markets_end_date ON markets(end_date);
CREATE INDEX IF NOT EXISTS idx_markets_polymarket_id ON markets(polymarket_id);
CREATE INDEX IF NOT EXISTS idx_markets_created_at ON markets(created_at);

-- Enable Row Level Security (RLS)
ALTER TABLE markets ENABLE ROW LEVEL SECURITY;

-- Create a policy to allow service role to do everything
CREATE POLICY "Allow service role full access" ON markets
    FOR ALL
    TO service_role
    USING (true)
    WITH CHECK (true);

-- Optional: Create a policy for anon/authenticated users to read
CREATE POLICY "Allow public read access" ON markets
    FOR SELECT
    TO anon, authenticated
    USING (true);

-- Create a function to automatically update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create a trigger to call the function
DROP TRIGGER IF EXISTS update_markets_updated_at ON markets;
CREATE TRIGGER update_markets_updated_at
    BEFORE UPDATE ON markets
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

