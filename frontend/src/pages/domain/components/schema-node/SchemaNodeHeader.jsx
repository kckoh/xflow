import { ChevronDown, ChevronUp, Archive, Database } from "lucide-react";
import { SiPostgresql, SiMongodb, SiMysql, SiApachekafka } from "@icons-pack/react-simple-icons";
import clsx from "clsx";

export const getStyleConfig = (platform) => {
    const p = platform?.toLowerCase() || "";
    if (p.includes("s3") || p.includes("archive")) {
        return {
            bgColor: "bg-orange-50",
            borderColor: "border-orange-200",
            iconColor: "text-orange-600",
            headerColor: "bg-orange-50",
        };
    }
    if (p.includes("mongo")) {
        return {
            bgColor: "bg-green-50",
            borderColor: "border-green-200",
            iconColor: "text-green-600",
            headerColor: "bg-green-50",
        };
    }
    if (p.includes("transform")) {
        return {
            bgColor: "bg-purple-50",
            borderColor: "border-purple-200",
            iconColor: "text-purple-600",
            headerColor: "bg-purple-50",
        };
    }
    return {
        bgColor: "bg-blue-50",
        borderColor: "border-blue-200",
        iconColor: "text-blue-600",
        headerColor: "bg-blue-50",
    };
};

export const getPlatformIcon = (platform) => {
    const p = platform?.toLowerCase() || "";
    if (p.includes("s3") || p.includes("archive")) return Archive;
    if (p.includes("postgres")) return SiPostgresql;
    if (p.includes("mongo")) return SiMongodb;
    if (p.includes("mysql")) return SiMysql;
    if (p.includes("kafka")) return SiApachekafka;
    return Database;
};

export const SchemaNodeHeader = ({ data, expanded, onToggle, id }) => {
    const sourcePlatform = data.platform || "PostgreSQL";
    const label = data.label || "Unknown Table";
    const config = getStyleConfig(sourcePlatform);
    const IconComponent = getPlatformIcon(sourcePlatform);

    return (
        <div className="flex rounded-t-lg overflow-hidden border-b border-gray-100">
            {/* Icon Area */}
            <div
                className={clsx(
                    config.bgColor,
                    config.borderColor,
                    "flex items-center justify-center px-3 py-3 border-r"
                )}
            >
                <IconComponent className={clsx("w-5 h-5", config.iconColor)} />
            </div>

            {/* Label & Toggle Area */}
            <div className="flex-1 flex items-center justify-between px-3 py-2 bg-white">
                <div className="flex flex-col min-w-0">
                    <span className="text-sm font-semibold text-gray-900 truncate" title={label}>
                        {label}
                    </span>
                </div>

                {/* Toggle Schema Button */}
                <button
                    className="ml-2 p-1 hover:bg-gray-100 rounded transition-colors"
                    onClick={(e) => {
                        e.stopPropagation();
                        onToggle(!expanded);
                        if (data.onToggleExpand) data.onToggleExpand(id, !expanded);
                    }}
                >
                    {expanded ? (
                        <ChevronUp className="w-4 h-4 text-gray-500" />
                    ) : (
                        <ChevronDown className="w-4 h-4 text-gray-500" />
                    )}
                </button>
            </div>
        </div>
    );
};
