import React, { useState } from 'react';
import './App.css';
import axios from 'axios';

const API_BASE_URL = process.env.REACT_APP_API_URL || 'http://localhost:8000';

function App() {
  const [naturalLanguage, setNaturalLanguage] = useState('');
  const [sourceType, setSourceType] = useState('csv');
  const [file, setFile] = useState(null);
  const [tableName, setTableName] = useState('');
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState(null);
  const [error, setError] = useState(null);
  const [schema, setSchema] = useState(null);
  const pipelineId = result?.pipeline_id;
  const dashUrl = pipelineId ? `${API_BASE_URL}/dash/?pipeline_id=${pipelineId}` : null;

  const handleFileChange = (e) => {
    setFile(e.target.files[0]);
  };

  const inferSchema = async () => {
    if (sourceType === 'csv' && !file) {
      setError('Please upload a CSV file');
      return;
    }
    if (sourceType === 'postgres' && !tableName.trim()) {
      setError('Please enter a table name');
      return;
    }

    setLoading(true);
    setError(null);
    setSchema(null);

    try {
      if (sourceType === 'csv') {
        const formData = new FormData();
        formData.append('file', file);

        const response = await axios.post(
          `${API_BASE_URL}/api/schema/infer-csv`,
          formData,
          {
            headers: {
              'Content-Type': 'multipart/form-data',
            },
          }
        );

        setSchema(response.data.schema);
      } else {
        const sourceConfig = {
          table_name: tableName.trim(),
        };

        if (process.env.REACT_APP_SUPABASE_URL) {
          sourceConfig.supabase_url = process.env.REACT_APP_SUPABASE_URL;
        }
        if (process.env.REACT_APP_SUPABASE_KEY) {
          sourceConfig.supabase_key = process.env.REACT_APP_SUPABASE_KEY;
        }

        const response = await axios.post(
          `${API_BASE_URL}/api/schema/infer`,
          {
            source_type: sourceType,
            source_config: sourceConfig,
          }
        );

        setSchema(response.data.schema);
      }
    } catch (err) {
      setError(err.response?.data?.detail || err.message || 'Failed to infer schema');
    } finally {
      setLoading(false);
    }
  };

  const generatePipeline = async () => {
    if (!naturalLanguage.trim()) {
      setError('Please enter a natural language request');
      return;
    }

    if (sourceType === 'csv' && !file) {
      setError('Please upload a CSV file');
      return;
    }
    if (sourceType === 'postgres' && !tableName.trim()) {
      setError('Please enter a table name');
      return;
    }

    setLoading(true);
    setError(null);
    setResult(null);

    try {
      let response;

      if (sourceType === 'csv') {
        // For CSV, use the file upload endpoint
        const formData = new FormData();
        formData.append('file', file);
        formData.append('natural_language', naturalLanguage);
        formData.append('source_type', sourceType);

        response = await axios.post(
          `${API_BASE_URL}/api/pipeline/generate-csv`,
          formData,
          {
            headers: {
              'Content-Type': 'multipart/form-data',
            },
          }
        );
      } else {
        const sourceConfig = {
          table_name: tableName.trim(),
        };

        if (process.env.REACT_APP_SUPABASE_URL) {
          sourceConfig.supabase_url = process.env.REACT_APP_SUPABASE_URL;
        }
        if (process.env.REACT_APP_SUPABASE_KEY) {
          sourceConfig.supabase_key = process.env.REACT_APP_SUPABASE_KEY;
        }

        const requestData = {
          natural_language: naturalLanguage,
          source_type: sourceType,
          source_config: sourceConfig,
        };

        // If we have schema, include it in the request
        if (schema) {
          requestData.schema = schema;
        }

        response = await axios.post(
          `${API_BASE_URL}/api/pipeline/generate`,
          requestData
        );
      }

      setResult(response.data);
    } catch (err) {
      setError(err.response?.data?.detail || err.message || 'Failed to generate pipeline');
    } finally {
      setLoading(false);
    }
  };

  const downloadPipeline = () => {
    if (!result) {
      return;
    }

    const pipeline = result.pipeline || result;
    const code = pipeline?.code;

    if (!code) {
      setError('No pipeline code available to download');
      return;
    }

    const lang = (pipeline.language || 'python').toLowerCase();
    let ext = 'txt';
    if (lang.includes('python') || lang === 'py') {
      ext = 'py';
    } else if (lang.includes('sql')) {
      ext = 'sql';
    }

    // Create a simple, safe filename from the natural language prompt
    const baseName = (naturalLanguage || 'pipeline')
      .slice(0, 40)
      .toLowerCase()
      .replace(/\s+/g, '-')
      .replace(/[^a-z0-9\-]/g, '') || 'pipeline';

    const blob = new Blob([code], { type: 'text/plain;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${baseName}.${ext}`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };

  return (
    <div className="App">
      <div className="container">
        <header className="header">
          <h1>🚀 Datagrep</h1>
          <p className="subtitle">AI-powered data pipeline generator</p>
        </header>

        <div className="main-content">
          <div className="input-section">
            <div className="form-group">
              <label htmlFor="sourceType">Data Source Type</label>
              <select
                id="sourceType"
                value={sourceType}
                onChange={(e) => setSourceType(e.target.value)}
                className="select-input"
              >
                <option value="csv">CSV File</option>
                <option value="postgres">PostgreSQL (Supabase)</option>
              </select>
            </div>

            {sourceType === 'csv' && (
              <div className="form-group">
                <label htmlFor="file">Upload CSV File</label>
                <input
                  id="file"
                  type="file"
                  accept=".csv"
                  onChange={handleFileChange}
                  className="file-input"
                />
                {file && <p className="file-name">Selected: {file.name}</p>}
              </div>
            )}

            {sourceType === 'postgres' && (
              <div className="form-group">
                <label htmlFor="tableName">Supabase Table Name</label>
                <input
                  id="tableName"
                  type="text"
                  value={tableName}
                  onChange={(e) => setTableName(e.target.value)}
                  placeholder="e.g., employees"
                  className="text-input"
                />
              </div>
            )}

            <div className="form-group">
              <label htmlFor="nlInput">Describe what you want to do with the data</label>
              <textarea
                id="nlInput"
                value={naturalLanguage}
                onChange={(e) => setNaturalLanguage(e.target.value)}
                placeholder="e.g., Filter rows where age is greater than 25, then group by department and calculate average salary"
                className="textarea-input"
                rows="4"
              />
            </div>

            <div className="button-group">
              <button
                onClick={inferSchema}
                disabled={
                  loading ||
                  (sourceType === 'csv' && !file) ||
                  (sourceType === 'postgres' && !tableName.trim())
                }
                className="btn btn-secondary"
              >
                {loading ? 'Inferring...' : 'Infer Schema'}
              </button>
              <button
                onClick={generatePipeline}
                disabled={
                  loading ||
                  !naturalLanguage.trim() ||
                  (sourceType === 'postgres' && !tableName.trim())
                }
                className="btn btn-primary"
              >
                {loading ? 'Generating...' : 'Generate Pipeline'}
              </button>
            </div>
          </div>

          {error && (
            <div className="error-message">
              <strong>Error:</strong> {error}
            </div>
          )}

          {schema && (
            <div className="result-section">
              <h2>Inferred Schema</h2>
              <div className="schema-display">
                <pre>{JSON.stringify(schema, null, 2)}</pre>
              </div>
            </div>
          )}

          {result && (
            <div className="result-section">
              <h2>Generated Pipeline</h2>
              <div className="pipeline-info">
                <p><strong>Language:</strong> {result.language || result.pipeline?.language}</p>
                <p><strong>Description:</strong> {result.pipeline?.description || result.description}</p>
              </div>
              <div className="code-display">
                <pre>
                  <code>{result.pipeline?.code || result.code}</code>
                </pre>
              </div>
              <div className="button-group" style={{ marginTop: '12px' }}>
                <button
                  onClick={downloadPipeline}
                  className="btn btn-secondary"
                >
                  Download {result.language || result.pipeline?.language || 'python'} file
                </button>
              </div>
              {result.pipeline?.dependencies && result.pipeline.dependencies.length > 0 && (
                <div className="dependencies">
                  <strong>Dependencies:</strong>
                  <ul>
                    {result.pipeline.dependencies.map((dep, idx) => (
                      <li key={idx}>{dep}</li>
                    ))}
                  </ul>
                </div>
              )}
              {dashUrl && (
                <div className="pipeline-visualization">
                  <h3>Analytics Dashboard</h3>
                  <iframe
                    title="Analytics dashboard"
                    src={dashUrl}
                    className="pipeline-visualization-frame"
                  />
                  <div className="button-group" style={{ marginTop: '12px' }}>
                    <a
                      className="btn btn-secondary"
                      href={dashUrl}
                      target="_blank"
                      rel="noreferrer"
                    >
                      Open interactive view
                    </a>
                  </div>
                </div>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

export default App;
