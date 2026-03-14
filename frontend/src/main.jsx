import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { BrowserRouter } from 'react-router-dom'
import { PipelineConfigProvider } from './context/PipelineConfigContext'
import App from './App'
import './index.css'

createRoot(document.getElementById('root')).render(
  <StrictMode>
    <BrowserRouter>
      <PipelineConfigProvider>
        <App />
      </PipelineConfigProvider>
    </BrowserRouter>
  </StrictMode>,
)