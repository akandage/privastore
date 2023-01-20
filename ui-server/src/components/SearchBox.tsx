import React from 'react';
import * as styles from './SearchBox.scss';

import RoundButton from './RoundButton';

type SearchBoxProps = {
    placeholder?: string
}

export default function SearchBox({ placeholder='Search' }: SearchBoxProps) {
    return <div className={styles.searchBox}>
        <RoundButton img="images/icons/search.svg" tooltip="Search" />
        <input type="text" name="search" placeholder={placeholder}></input>
        <RoundButton img="images/icons/switches.svg" tooltip="Search Settings" />
    </div>
}