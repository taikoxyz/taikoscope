import React from 'react';
import { Routes, Route, Navigate } from 'react-router-dom';
import { DashboardLayout } from './components/layout/DashboardLayout';
import { DashboardRoute } from './components/routes/DashboardRoute';
import { TableRoute } from './components/routes/TableRoute';
import { ThemeProvider } from './contexts/ThemeContext';

const App: React.FC = () => {
  return (
    <ThemeProvider>
      <Routes>
        <Route path="/" element={<DashboardLayout />}>
          <Route index element={<DashboardRoute />} />
          <Route path="table/:tableType" element={<TableRoute />} />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Route>
      </Routes>
    </ThemeProvider>
  );
};

export default App;
