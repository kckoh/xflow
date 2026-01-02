import {
    ChevronDown, ChevronUp, Archive, Database,
    Columns, Filter, ArrowRightLeft, GitMerge, BarChart3, ArrowUpDown, Combine, Activity
} from "lucide-react";
import { SiMongodb, SiPostgresql, SiMysql } from "@icons-pack/react-simple-icons";
import clsx from "clsx";

export const getStyleConfig = (platform) => {
    const p = platform?.toLowerCase() || "";

    // S3 / Archive
    if (p.includes("s3") || p.includes("archive")) {
        return {
            bgColor: "bg-orange-50",
            borderColor: "border-orange-200",
            iconColor: "text-orange-600",
            headerColor: "bg-orange-50",
        };
    }

    // Databases (Mongo, Postgres, MySQL)
    if (p.includes("mongo")) {
        return {
            bgColor: "bg-green-50",
            borderColor: "border-green-200",
            iconColor: "text-green-600",
            headerColor: "bg-green-50",
        };
    }

    // Kafka
    if (p.includes("kafka")) {
        return {
            bgColor: "bg-gray-50",
            borderColor: "border-gray-200",
            iconColor: "text-gray-600",
            headerColor: "bg-gray-50",
        };
    }

    // Transforms (Generic 'transform' or specific types)
    if (
        p.includes("transform") ||
        p.includes("select") ||
        p.includes("join") ||
        p.includes("filter") ||
        p.includes("union") ||
        p.includes("map") ||
        p.includes("aggregate") ||
        p.includes("sort")
    ) {
        return {
            bgColor: "bg-purple-50",
            borderColor: "border-purple-200",
            iconColor: "text-purple-600",
            headerColor: "bg-purple-50",
        };
    }

    // Default (Postgres etc)
    return {
        bgColor: "bg-blue-50",
        borderColor: "border-blue-200",
        iconColor: "text-blue-600",
        headerColor: "bg-blue-50",
    };
};

export const getPlatformIcon = (platform) => {
    const p = platform?.toLowerCase() || "";

    // Transforms
    if (p.includes("select")) return Columns;
    if (p.includes("filter")) return Filter;
    if (p.includes("union")) return Combine;
    if (p.includes("map")) return ArrowRightLeft;
    if (p.includes("join")) return GitMerge;
    if (p.includes("aggregate")) return BarChart3;
    if (p.includes("sort")) return ArrowUpDown;

    // Sources
    if (p.includes("s3")) return Archive;
    if (p.includes("kafka")) return Activity;

    // Databases
    if (p.includes("mongo")) return SiMongodb;
    if (p.includes("postgres")) return SiPostgresql;
    if (p.includes("mysql")) return SiMysql;

    // Default Database for other DBs
    return Database;
};

export const SchemaNodeHeader = ({ data, expanded, onToggle, id }) => {
    let sourcePlatform = data.platform || "PostgreSQL";
    const label = data.label || "Unknown Table";

    // Deleted override logic to allow Icon (Source) and Label (Target) to differ
    // as per user request (e.g. Mongo Icon + (S3) Text)

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
