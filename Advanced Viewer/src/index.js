import React from 'react';
import { createRoot } from 'react-dom/client';
import App from './app';

// Initialize the Zendesk App Framework
const client = ZAFClient.init();

if (!client) {
  document.body.innerHTML = '<p style="padding:20px;color:#dc2626;font-family:sans-serif;">This app must be run inside Zendesk.</p>';
} else {
  const container = document.getElementById('root');
  if (container) {
    const root = createRoot(container);
    root.render(<App client={client} />);
  }
}