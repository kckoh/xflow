import { useState } from "react";
import TableColumnSidebar from "./components/TableColumnSidebar";
import QueryEditor from "./components/QueryEditor";

export default function QueryPage() {
    const [selectedTable, setSelectedTable] = useState(null);

    return (
        <div className="flex h-[calc(100vh-80px)] overflow-hidden">
            {/* Table & Column Sidebar - 왼쪽 */}
            <TableColumnSidebar
                selectedTable={selectedTable}
                onSelectTable={setSelectedTable}
            />

            {/* Query Editor - 메인 영역 */}
            <QueryEditor selectedTable={selectedTable} />
        </div>
    );
}
