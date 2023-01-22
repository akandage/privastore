import React from 'react';
import { Outlet } from 'react-router-dom';
import * as styles from './App.scss';

import Header from './header/Header';
import { Sidebar } from './sidebar/Sidebar'

export default function App() {
    return (
        <div className={styles.app}>
            <Header />
            <div className={styles.appBody}>
                <Sidebar.Sidebar />
                <div className={styles.appBodyMain}>
                    <Outlet />
                </div>
            </div>
        </div>
    );
}