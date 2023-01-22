import React from 'react';
import ReactDOM from 'react-dom';
import { createBrowserRouter, Route, RouterProvider } from 'react-router-dom';
import App from './components/App';
import FilesView from './components/views/FilesView';
import SettingsView from './components/views/SettingsView';
import TrashView from './components/views/TrashView';
import UploadsView from './components/views/UploadsView';

const router = createBrowserRouter([
    {
        path: "/",
        element: <App />,
        children: [
            {
                path: "/",
                element: <FilesView path="/" />
            },
            {
                path: "/files",
                element: <FilesView path="/" />
            },
            {
                path: "/recent",
                element: <FilesView path="/" showRecent />
            },
            {
                path: "/uploads",
                element: <UploadsView />
            },
            {
                path: "/trash",
                element: <TrashView />
            },
            {
                path: "/settings",
                element: <SettingsView />
            }
        ]
    },
]);

ReactDOM.render(<RouterProvider router={router} />, document.getElementById('root'));