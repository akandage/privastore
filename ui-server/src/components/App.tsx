import React from 'react';
import * as styles from './App.scss';

import Header from './header/Header';

export default function App() {
    return (
        <div className={styles.app}>
            <Header />
        </div>
    );
}