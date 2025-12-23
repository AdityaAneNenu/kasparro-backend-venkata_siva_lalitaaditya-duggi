-- Kaspero Database Initialization Script

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create indexes for better query performance
-- (Tables are created by SQLAlchemy, this adds additional optimizations)

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO kaspero;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO kaspero;
