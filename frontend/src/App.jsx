import { NavLink, Route, Routes } from 'react-router-dom'
import NavDropdown from './components/NavDropdown'
import HomeDashboard from './pages/HomeDashboard'
import SettingsPage from './pages/SettingsPage'
import PipelineDashboard from './pages/PipelineDashboard'
import AnalysisView from './pages/AnalysisView'
import DraftingView from './pages/DraftingView'

function App() {
  return (
    <div className="app-shell">
      <nav className="top-nav">
        <span className="brand">USL Signal Hunter v2</span>
        <NavLink to="/" end className={({ isActive }) => `nav-link${isActive ? ' active' : ''}`}>
          Home
        </NavLink>
        <NavLink to="/settings" className={({ isActive }) => `nav-link${isActive ? ' active' : ''}`}>
          Settings
        </NavLink>
        <NavDropdown sourceType="leadspicker" label="Leadspicker" />
        <NavDropdown sourceType="crunchbase" label="Crunchbase" />
        <NavDropdown sourceType="news" label="News" />
      </nav>

      <main className="main-wrap">
        <Routes>
          <Route path="/" element={<HomeDashboard />} />
          <Route path="/settings" element={<SettingsPage />} />
          <Route path="/pipeline/:type" element={<PipelineDashboard />} />
          <Route path="/pipeline/new" element={<PipelineDashboard />} />
          <Route path="/analyze/:pipelineKey/:batchId" element={<AnalysisView />} />
          <Route path="/draft/:batchId" element={<DraftingView />} />
        </Routes>
      </main>
    </div>
  )
}

export default App
