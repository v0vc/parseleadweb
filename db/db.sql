CREATE TABLE input1 (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    firstdate TEXT,
    fio TEXT NOT NULL,
    email TEXT,
    tel TEXT NOT NULL,
    status TEXT,
    result TEXT,
    comment TEXT,
    isopen TEXT,
    opendate TEXT,
    UNIQUE(fio,tel)
);
CREATE TABLE input2 (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    active TEXT,
    moddate TEXT,
    id TEXT,
    fio TEXT NOT NULL,
    email TEXT,
    tel TEXT NOT NULL,
    page TEXT,
    utm_source TEXT,
    utm_medium TEXT,
    utm_campaign TEXT,
    utm_content TEXT,
    utm_term TEXT,
    UNIQUE(fio,tel)
);