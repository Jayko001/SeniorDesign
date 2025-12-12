# Datagrep MVP Setup Guide

## Quick Start

### 1. Prerequisites
- Docker and Docker Compose installed
- OpenAI API key (get one at https://platform.openai.com/api-keys)

### 2. Environment Setup

**IMPORTANT:** Docker Compose reads environment variables from a `.env` file in the **root directory** (where `docker-compose.yml` is located).

Create a `.env` file in the `datagrep/` root directory:

```bash
# In the datagrep directory (root, not backend/)
cat > .env << EOF
OPENAI_API_KEY=sk-your-actual-key-here
SUPABASE_URL=your_supabase_url_here
SUPABASE_KEY=your_supabase_key_here
EOF
```

Or manually create `.env` in the root directory with:
```
OPENAI_API_KEY=sk-your-actual-key-here
```

**Note:** The `.env` file should be in the same directory as `docker-compose.yml`, not in the `backend/` folder.

### 3. Start the Application

```bash
docker-compose up --build
```

This will:
- Build and start the backend API (port 8000)
- Build and start the frontend (port 3000)
- Start a PostgreSQL database (port 5432)

### 4. Access the Application

- Frontend: http://localhost:3000
- Backend API: http://localhost:8000
- API Docs: http://localhost:8000/docs

## Testing the MVP

### Test with CSV File

1. Go to http://localhost:3000
2. Select "CSV File" as data source
3. Upload a CSV file (you can use `sample_data/employees.csv`)
4. Click "Infer Schema" to see the inferred schema
5. Enter a natural language request like:
   - "Filter employees where age is greater than 30"
   - "Group by department and calculate average salary"
   - "Filter where department is Engineering and salary is greater than 70000"
6. Click "Generate Pipeline" to get your Python code

### Test with PostgreSQL

1. Set up Supabase connection in `backend/.env`:
   ```
   SUPABASE_URL=https://your-project.supabase.co
   SUPABASE_KEY=your-anon-key
   ```

2. In the frontend, select "PostgreSQL (Supabase)"
3. Enter your table name
4. Enter a natural language request
5. Generate your SQL pipeline

## Development Mode

### Backend Only
```bash
cd backend
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
uvicorn main:app --reload
```

### Frontend Only
```bash
cd frontend
npm install
npm start
```

## Troubleshooting

### Port Already in Use
If ports 3000, 8000, or 5432 are already in use, modify `docker-compose.yml` to use different ports.

### OpenAI API Errors

**"Connection error" or "OPENAI_API_KEY not set"**
- Make sure you have a `.env` file in the **root directory** (same level as `docker-compose.yml`)
- The `.env` file should contain: `OPENAI_API_KEY=sk-your-key-here`
- Restart Docker containers after creating/updating `.env`: `docker-compose restart backend`
- Verify your API key is correct and has credits
- Check your OpenAI account has proper permissions
- If using Docker, ensure the environment variable is passed: `docker-compose up --build`

### Database Connection Issues
- Check PostgreSQL container is running: `docker-compose ps`
- Verify connection credentials in `.env`

## Next Steps

- Add more data transformations
- Implement pipeline execution
- Add pipeline testing and validation
- Create pipeline templates
- Add user authentication

