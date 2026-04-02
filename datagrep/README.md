# Datagrep MVP

**[User Guide & Manual](USER_GUIDE.md)**  
Find step-by-step instructions, FAQ, and troubleshooting for using Datagrep's web interface and Slack bot.

AI-powered data pipeline generator that converts natural language requests into executable data pipelines.

## Features

- **Natural Language Processing**: Describe your data pipeline needs in plain English
- **Schema Inference**: Automatically infer schemas from CSV files and PostgreSQL databases
- **Pipeline Generation**: Generate Python or SQL pipelines using OpenAI GPT-4
- **Multiple Data Sources**: Support for CSV files and PostgreSQL (Supabase)
- **Docker Support**: Fully containerized for easy deployment and scaling

## Architecture

```
datagrep/
├── backend/          # FastAPI backend
│   ├── main.py      # API server
│   ├── services/    # Core services
│   │   ├── schema_inference.py
│   │   ├── pipeline_generator.py
│   │   └── supabase_client.py
│   └── requirements.txt
├── frontend/        # React frontend
│   └── src/
└── docker-compose.yml
```

## Prerequisites

- Docker and Docker Compose
- OpenAI API key
- (Optional) Supabase account for PostgreSQL connections

## Setup

1. **Clone and navigate to the project**:
   ```bash
   cd datagrep
   ```

2. **Set up environment variables**:
   ```bash
   cp backend/.env.example backend/.env
   ```
   
   Edit `backend/.env` and add your OpenAI API key:
   ```
   OPENAI_API_KEY=your_openai_api_key_here
   ```

3. **Start the services**:
   ```bash
   docker-compose up --build
   ```

   This will start:
   - Backend API on `http://localhost:8000`
   - Frontend on `http://localhost:3000`
   - PostgreSQL database on `localhost:5432`

## Usage

### Web Interface

1. Open `http://localhost:3000` in your browser
2. Select your data source type (CSV or PostgreSQL)
3. Upload a CSV file or configure PostgreSQL connection
4. Enter your natural language request (e.g., "Filter rows where age > 25, then group by department")
5. Click "Generate Pipeline" to get your code

### API Endpoints

#### Health Check
```bash
GET http://localhost:8000/health
```

#### Infer Schema from CSV
```bash
POST http://localhost:8000/api/schema/infer-csv
Content-Type: multipart/form-data

file: <csv_file>
```

#### Generate Pipeline
```bash
POST http://localhost:8000/api/pipeline/generate
Content-Type: application/json

{
  "natural_language": "Filter rows where age > 25",
  "source_type": "csv",
  "source_config": {
    "file_path": "data.csv"
  }
}
```

## Development

### Backend Development

```bash
cd backend
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
uvicorn main:app --reload
```

### Frontend Development

```bash
cd frontend
npm install
npm start
```

## Project Structure

- **Backend**: FastAPI application with services for schema inference and pipeline generation
- **Frontend**: React application with a simple, modern UI
- **Services**:
  - `schema_inference.py`: Infers schemas from CSV and PostgreSQL
  - `pipeline_generator.py`: Uses OpenAI to generate pipeline code
  - `supabase_client.py`: Manages Supabase connections

## Next Steps

- [ ] Add more data source types (APIs, cloud storage)
- [ ] Implement pipeline execution and testing
- [ ] Add pipeline versioning and history
- [ ] Improve error handling and validation
- [ ] Add authentication and user management
- [ ] Deploy to production environment

## License

This is a Senior Design project for the University of Cincinnati.

