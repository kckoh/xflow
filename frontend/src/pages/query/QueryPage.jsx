import { useState } from "react";
import DatabaseSidebar from "./components/DatabaseSidebar";
import TableColumnSidebar from "./components/TableColumnSidebar";
import QueryEditor from "./components/QueryEditor";

export default function QueryPage() {
    const [selectedDatabase, setSelectedDatabase] = useState(null);
    const [selectedTable, setSelectedTable] = useState(null);

    return (
        <div className="flex h-[calc(100vh-80px)] overflow-hidden">
            {/* Database Sidebar - 왼쪽 첫번째 */}
            <DatabaseSidebar
                selectedDatabase={selectedDatabase}
                onSelectDatabase={setSelectedDatabase}
            />

            {/* Table & Column Sidebar - 왼쪽 두번째 */}
            <TableColumnSidebar
                selectedDatabase={selectedDatabase}
                selectedTable={selectedTable}
                onSelectTable={setSelectedTable}
            />

            {/* Query Editor - 메인 영역 */}
            <QueryEditor
                selectedTable={selectedTable}
                selectedDatabase={selectedDatabase}
            />
        </div>
    );
}
