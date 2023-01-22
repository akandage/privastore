import React from 'react';
import * as styles from './FilesView.scss';

type FilesViewProps = {
    path: string,
    showFavourites?: boolean,
    showRecent?: boolean
}

export default function FilesView({ path, showFavourites = false, showRecent = false }: FilesViewProps)
{
    return <span>Files</span>
}