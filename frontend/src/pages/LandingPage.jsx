import { useNavigate } from "react-router-dom";
import { useAuth } from "../context/AuthContext";
import {
    Database,
    Workflow,
    FolderSearch,
    GitMerge,
    BarChart3,
    ArrowRight,
    Zap,
    Globe,
    Share2,
    Search,
    ChevronLeft,
    ChevronRight
} from "lucide-react";
import { useEffect, useState } from "react";
import logo from "../assets/logo.png";
import icon from "../assets/icon.png";
import landingVideo from "../assets/landingVideo.mp4";
import visual1 from "../assets/visual/visual1.png";
import visual2 from "../assets/visual/visual2.png";
import visual3 from "../assets/visual/visual3.png";
import kc from "../assets/person/kc.png";
import mh from "../assets/person/mh.jpg";
import sc from "../assets/person/sc.png";
import jy from "../assets/person/jy.png";
import sk from "../assets/person/sk.jpg";

function LandingPage() {
    const navigate = useNavigate();
    const { sessionId } = useAuth();
    const [activeStickySection, setActiveStickySection] = useState(0);

    // Redirect if already logged in
    useEffect(() => {
        if (sessionId) {
            navigate("/dataset");
        }
    }, [sessionId, navigate]);

    // Scroll spy for sticky sections
    useEffect(() => {
        const handleScroll = () => {
            const sections = document.querySelectorAll('.sticky-section-trigger');
            sections.forEach((section, index) => {
                const rect = section.getBoundingClientRect();
                if (rect.top <= window.innerHeight / 2 && rect.bottom >= window.innerHeight / 2) {
                    setActiveStickySection(index);
                }
            });
        };
        window.addEventListener('scroll', handleScroll);
        return () => window.removeEventListener('scroll', handleScroll);
    }, []);

    return (
        <div className="min-h-screen bg-white text-[#202124] font-sans selection:bg-blue-100 selection:text-blue-900">

            {/* Navigation (Minimalist) */}
            <nav className="fixed top-0 left-0 right-0 z-50 bg-white/80 backdrop-blur-md transition-all duration-300">
                <div className="max-w-[1440px] mx-auto px-6 h-16 flex items-center justify-between">
                    <div className="flex items-center gap-2">
                        <img src={icon} alt="XFlow" className="h-10" />
                        <h1 className="text-2xl font-bold">XFlow</h1>
                    </div>
                    <div className="hidden md:flex items-center gap-8 text-sm font-medium text-[#5F6368]">
                        <a href="#solutions" className="hover:text-[#202124] transition-colors">Solutions</a>
                        <a href="#platform" className="hover:text-[#202124] transition-colors">Platform</a>
                        <a href="#resources" className="hover:text-[#202124] transition-colors">Resources</a>
                    </div>
                    <button
                        onClick={() => navigate("/login")}
                        className="bg-[#202124] text-white px-6 py-2.5 rounded-full text-sm font-medium hover:bg-gray-800 transition-colors"
                    >
                        Start XFlow
                    </button>
                </div>
            </nav>

            {/* Hero Section */}
            <section className="relative pt-32 pb-24 px-6 flex flex-col items-center justify-center text-center overflow-hidden">
                {/* Subtle Background Particles */}
                <div className="absolute inset-0 z-0 pointer-events-none">
                    <div className="absolute top-20 left-[20%] w-72 h-72 bg-blue-100/50 rounded-full blur-3xl mix-blend-multiply animate-pulse" />
                    <div className="absolute bottom-20 right-[20%] w-96 h-96 bg-purple-100/50 rounded-full blur-3xl mix-blend-multiply animate-pulse delay-1000" />
                </div>

                <div className="relative z-10 max-w-5xl mx-auto flex flex-col items-center">
                    <style>{`
                        @keyframes shimmer-rain {
                            0% { transform: translateY(-50px); opacity: 0; }
                            10% { opacity: 1; }
                            90% { opacity: 1; }
                            100% { transform: translateY(600px); opacity: 0; }
                        }
                        @keyframes twinkle {
                            0% { opacity: 0.3; transform: scale(0.8); box-shadow: 0 0 0 0 rgba(255, 255, 255, 0); }
                            50% { 
                                opacity: 1; 
                                transform: scale(1.1); 
                                /* Deep solid center feeling usually comes from high contrast. 
                                   We simulate "darker center" relative to bright bloom by making the shadow intense but the inner bg slightly off-white or just pure white with massive bloom. 
                                   User asked for "center shade deep, outer white low brightness". 
                                   Let's try a radial gradient or intense shadow. */
                                box-shadow: 0 0 30px 10px rgba(255, 255, 255, 0.8); /* Wider glow radius (30px) and spread (10px) */
                                background: #ffffff;
                            }
                            100% { opacity: 0.3; transform: scale(0.8); box-shadow: 0 0 0 0 rgba(255, 255, 255, 0); }
                        }
                        .star-rain-bg {
                            background: radial-gradient(circle, white 10%, transparent 10%);
                            background-size: 10px 10px; 
                            position: absolute;
                            top: 0; left: 0; right: 0; bottom: 0;
                            opacity: 0.5;
                        }
                        .floating-star {
                            position: absolute;
                            background: white;
                            border-radius: 50%;
                        }
                    `}</style>
                    <br /><br /><br />
                    <div className="relative mb-4 group w-full flex justify-center">
                        {/* Base Logo (Black) - Acts as the container shape */}
                        <div className="relative h-48 md:h-48 w-auto aspect-[7/2]">
                            {/* 1. The Mask Container: Everything inside here is clipped to the logo shape */}
                            <div
                                className="absolute inset-0 z-10"
                                style={{
                                    maskImage: `url(${logo})`,
                                    WebkitMaskImage: `url(${logo})`,
                                    maskSize: 'contain',
                                    WebkitMaskSize: 'contain',
                                    maskRepeat: 'no-repeat',
                                    WebkitMaskRepeat: 'no-repeat',
                                    maskPosition: 'center',
                                    WebkitMaskPosition: 'center',
                                    backgroundColor: 'black' // Fallback / Base color
                                }}
                            >
                                {/* 2. The Animation: Floating Stars INSIDE the logo */}
                                <div className="absolute inset-0 overflow-hidden bg-slate-900">
                                    {/* Dynamic Floating Stars numbers*/}
                                    {[...Array(200)].map((_, i) => (
                                        <div
                                            key={i}
                                            className="absolute"
                                            style={{
                                                left: `${Math.random() * 100}%`,
                                                animation: `shimmer-rain ${Math.random() * 7 + 8}s linear infinite`, // Much slower: 8s to 15s
                                                animationDelay: `${Math.random() * 5}s`
                                            }}
                                        >
                                            <div
                                                className="floating-star"
                                                style={{
                                                    width: `${Math.random() * 6 + 2}px`,
                                                    height: `${Math.random() * 6 + 2}px`,
                                                    background: 'radial-gradient(circle, rgba(255,255,255,1) 30%, rgba(200,200,200,1) 100%)', // Core is bright white, edge is slightly dimmer
                                                    // Faster forceful twinkling against slow fall
                                                    animation: `twinkle ${Math.random() * 1.5 + 0.5}s ease-in-out infinite alternate`
                                                }}
                                            />
                                        </div>
                                    ))}
                                    {/* Add some twinkling dots */}
                                    {[...Array(30)].map((_, i) => (
                                        <div
                                            key={`dot-${i}`}
                                            className="absolute rounded-full bg-white animate-pulse"
                                            style={{
                                                top: `${Math.random() * 100}%`,
                                                left: `${Math.random() * 100}%`,
                                                width: `${Math.random() * 2 + 1}px`,
                                                height: `${Math.random() * 2 + 1}px`,
                                                animationDelay: `${Math.random() * 3}s`,
                                                opacity: Math.random() * 0.8
                                            }}
                                        />
                                    ))}
                                </div>
                            </div>

                            {/* Invisible Image to maintain layout size */}
                            <img src={logo} alt="XFlow" className="h-full w-auto object-contain opacity-0 pointer-events-none" />
                        </div>
                    </div>
                    <br />
                    <h1 className="text-5xl md:text-7xl font-[500] tracking-tight leading-[1.1] mb-6 text-[#202124]">
                        Experience detailed<br />
                        <span className="text-[#5F6368]">data orchestration.</span>
                    </h1>
                    <br />
                    <p className="text-xl md:text-2xl text-[#5F6368] mb-12 max-w-3xl mx-auto leading-relaxed font-light">
                        From collection to catalog, all in one service.<br />
                        The complete solution for your data journey.
                    </p>
                    <br />
                    <div className="flex flex-wrap justify-center gap-4">
                        <button
                            onClick={() => navigate("/login")}
                            className="bg-[#202124] text-white px-8 py-4 rounded-full text-lg font-medium hover:bg-gray-800 transition-all hover:scale-105"
                        >
                            Get Started
                        </button>
                        <button
                            className="bg-[#F1F3F4] text-[#202124] px-8 py-4 rounded-full text-lg font-medium hover:bg-gray-200 transition-all hover:scale-105"
                        >
                            Watch Video
                        </button>
                    </div>
                </div>
                <br /><br /><br /><br /><br />
                {/* Hero Visual Placeholder */}
                <div className="mt-20 w-full max-w-6xl mx-auto px-6">
                    <div className="aspect-[16/9] bg-[#F8F9FA] rounded-[32px] border border-gray-100 shadow-2xl flex items-center justify-center relative overflow-hidden group">
                        <video
                            src={landingVideo}
                            className="w-full h-full object-cover"
                            autoPlay
                            loop
                            muted
                            playsInline
                        />
                    </div>
                </div>
            </section>

            {/* Solutions Section (Sticky Visual, Scrolling Text) */}
            <section id="solutions" className="relative">
                <div className="max-w-[1440px] mx-auto flex flex-col lg:flex-row items-start"> {/* Added items-start to fix sticky */}

                    {/* Left: Scrolling Text Content */}
                    <div className="w-full lg:w-1/2 relative z-10 p-6 lg:p-0">
                        {/* Step 1: Challenge */}
                        <div className="sticky-section-trigger min-h-[80vh] flex flex-col justify-center lg:pl-12 lg:pr-6" data-index="0">
                            <div className="inline-flex items-center gap-2 px-3 py-1 rounded-full bg-red-50 text-red-600 text-sm font-medium mb-6 w-fit">
                                <Zap size={16} /> The Challenge
                            </div>
                            <h2 className="text-4xl md:text-5xl font-medium mb-6 leading-tight">
                                Scattered data,<br />
                                <span className="text-[#5F6368]">scattered focus.</span>
                            </h2>
                            <p className="text-xl text-[#5F6368] leading-relaxed max-w-md">
                                Companies want AI, but data is fragmented across DBs, APIs, and logs.
                                Integrating these silos is the first major hurdle.
                            </p>
                        </div>

                        {/* Step 2: Data Lake */}
                        <div className="sticky-section-trigger min-h-[80vh] flex flex-col justify-center lg:pl-12 lg:pr-6" data-index="1">
                            <div className="inline-flex items-center gap-2 px-3 py-1 rounded-full bg-blue-50 text-blue-600 text-sm font-medium mb-6 w-fit">
                                <Database size={16} /> Data Lake
                            </div>
                            <h2 className="text-4xl md:text-5xl font-medium mb-6 leading-tight">
                                Unified storage,<br />
                                <span className="text-[#5F6368]">limitless scale.</span>
                            </h2>
                            <p className="text-xl text-[#5F6368] leading-relaxed max-w-md">
                                XFlow gathers scattered big data into a single repository.
                                Experience flexibility and infinite scalability.
                            </p>
                        </div>

                        {/* Step 3: Catalog */}
                        <div className="sticky-section-trigger min-h-[80vh] flex flex-col justify-center lg:pl-12 lg:pr-6" data-index="2">
                            <div className="inline-flex items-center gap-2 px-3 py-1 rounded-full bg-purple-50 text-purple-600 text-sm font-medium mb-6 w-fit">
                                <Search size={16} /> Catalog
                            </div>
                            <h2 className="text-4xl md:text-5xl font-medium mb-6 leading-tight">
                                Find what matters,<br />
                                <span className="text-[#5F6368]">instantly.</span>
                            </h2>
                            <p className="text-xl text-[#5F6368] leading-relaxed max-w-md">
                                Collecting isn't enough. Our catalog automates classification and access control,
                                making discovery effortless.
                            </p>
                        </div>
                    </div>

                    {/* Right: Sticky Visual Content */}
                    <div className="hidden lg:flex w-1/2 sticky top-0 h-screen items-center justify-center p-6 lg:p-12">
                        <div className="relative w-full aspect-square md:aspect-[4/3] rounded-[48px] overflow-hidden shadow-2xl bg-white border border-gray-100">

                            {/* Visual 1 */}
                            <div className={`absolute inset-0 transition-opacity duration-700 ease-in-out bg-[#F1F3F4] flex items-center justify-center overflow-hidden ${activeStickySection === 0 ? 'opacity-100' : 'opacity-0'}`}>
                                <img src={visual1} alt="Problem Visualization" className="w-full h-full object-cover" />
                            </div>

                            {/* Visual 2 */}
                            <div className={`absolute inset-0 transition-opacity duration-700 ease-in-out bg-[#E8F0FE] flex items-center justify-center ${activeStickySection === 1 ? 'opacity-100' : 'opacity-0'}`}>
                                <img src={visual2} alt="Data Lake Architecture" className="w-full h-full object-cover" />
                            </div>

                            {/* Visual 3 */}
                            <div className={`absolute inset-0 transition-opacity duration-700 ease-in-out bg-[#F3E8FD] flex items-center justify-center ${activeStickySection === 2 ? 'opacity-100' : 'opacity-0'}`}>
                                <img src={visual3} alt="Catalog Interface" className="w-full h-full object-cover" />
                            </div>

                        </div>
                    </div>

                </div>
            </section>



            {/* Persona Carousel Section */}
            <section className="py-20 md:py-32 px-6 max-w-[1440px] mx-auto overflow-hidden">
                <div className="mb-12 flex items-end justify-between px-2">
                    <div>
                        <h2 className="text-3xl md:text-4xl font-medium mb-4">Built for every role</h2>
                        <p className="text-[#5F6368] text-lg max-w-xl">
                            Whether you build pipelines or analyze trends, XFlow streamlines your work.
                        </p>
                    </div>
                    {/* Navigation Buttons */}
                    <div className="hidden md:flex gap-4">
                        <button
                            onClick={() => document.getElementById('persona-scroll').scrollBy({ left: -400, behavior: 'smooth' })}
                            className="w-12 h-12 rounded-full bg-gray-100 hover:bg-gray-200 flex items-center justify-center transition-colors"
                        >
                            <ChevronLeft size={24} />
                        </button>
                        <button
                            onClick={() => document.getElementById('persona-scroll').scrollBy({ left: 400, behavior: 'smooth' })}
                            className="w-12 h-12 rounded-full bg-gray-100 hover:bg-gray-200 flex items-center justify-center transition-colors"
                        >
                            <ChevronRight size={24} />
                        </button>
                    </div>
                </div>

                {/* Horizontal Scroll Container */}
                <div
                    id="persona-scroll"
                    className="flex gap-6 overflow-x-auto snap-x snap-mandatory pb-12 hide-scrollbar"
                    style={{ scrollbarWidth: 'none', msOverflowStyle: 'none' }}
                >
                    {/* Card 1: PM & Fullstack Developer*/}
                    <div className="min-w-[85vw] md:min-w-[600px] snap-center">
                        <div className="aspect-[16/10] bg-[#F1F3F4] rounded-[32px] relative overflow-hidden group mb-6">
                            <img
                                src={kc}
                                alt="PM & Fullstack Developer"
                                className="absolute inset-0 w-full h-full object-cover transition-transform duration-700 group-hover:scale-105"
                            />
                            <div className="absolute inset-0 bg-black/20 group-hover:bg-black/10 transition-colors duration-500" />
                            <div className="absolute inset-0 flex items-center justify-center">
                                <h3 className="text-3xl md:text-4xl font-medium text-white drop-shadow-lg">PM & Fullstack Developer</h3>
                            </div>
                            <div className="absolute bottom-6 right-6 w-10 h-10 bg-white/30 backdrop-blur rounded-full flex items-center justify-center text-white">
                                <ArrowRight size={16} />
                            </div>
                        </div>
                        <h4 className="text-xl font-medium mb-2">PM & Fullstack Developer</h4>
                        <p className="text-[#5F6368]">
                            Automate complex ETL pipelines with drag-and-drop simplicity.
                            Monitor health and performance in real-time.
                        </p>
                    </div>

                    {/* Card 2:Fullstack Developer */}
                    <div className="min-w-[85vw] md:min-w-[600px] snap-center">
                        <div className="aspect-[16/10] bg-[#E8F0FE] rounded-[32px] relative overflow-hidden group mb-6">
                            <img
                                src={mh}
                                alt="Fullstack Developer"
                                className="absolute inset-0 w-full h-full object-cover transition-transform duration-700 group-hover:scale-105"
                            />
                            <div className="absolute inset-0 bg-black/20 group-hover:bg-black/10 transition-colors duration-500" />
                            <div className="absolute inset-0 flex items-center justify-center">
                                <h3 className="text-3xl md:text-4xl font-medium text-white drop-shadow-lg">Fullstack Developer</h3>
                            </div>
                            <div className="absolute bottom-6 right-6 w-10 h-10 bg-white/30 backdrop-blur rounded-full flex items-center justify-center text-white">
                                <ArrowRight size={16} />
                            </div>
                        </div>
                        <h4 className="text-xl font-medium mb-2">Fullstack Developer</h4>
                        <p className="text-[#5F6368]">
                            Access clean, cataloged data instantly.
                            Spend less time wrangling and more time modeling.
                        </p>
                    </div>

                    {/* Card 3: Fullstack Developer */}
                    <div className="min-w-[85vw] md:min-w-[600px] snap-center">
                        <div className="aspect-[16/10] bg-[#F3E8FD] rounded-[32px] relative overflow-hidden group mb-6">
                            <img
                                src={sc}
                                alt="Fullstack Developer"
                                className="absolute inset-0 w-full h-full object-cover transition-transform duration-700 group-hover:scale-105"
                            />
                            <div className="absolute inset-0 bg-black/20 group-hover:bg-black/10 transition-colors duration-500" />
                            <div className="absolute inset-0 flex items-center justify-center">
                                <h3 className="text-3xl md:text-4xl font-medium text-white drop-shadow-lg">Fullstack Developer</h3>
                            </div>
                            <div className="absolute bottom-6 right-6 w-10 h-10 bg-white/30 backdrop-blur rounded-full flex items-center justify-center text-white">
                                <ArrowRight size={16} />
                            </div>
                        </div>
                        <h4 className="text-xl font-medium mb-2">Fullstack Developer</h4>
                        <p className="text-[#5F6368]">
                            Trust your metrics with full lineage visibility.
                            Self-serve insights without waiting on engineering.
                        </p>
                    </div>
                    {/* Card 4: Fullstack Developer */}
                    <div className="min-w-[85vw] md:min-w-[600px] snap-center">
                        <div className="aspect-[16/10] bg-[#E6F4EA] rounded-[32px] relative overflow-hidden group mb-6">
                            <img
                                src={jy}
                                alt="Fullstack Developer"
                                className="absolute inset-0 w-full h-full object-cover transition-transform duration-700 group-hover:scale-105"
                            />
                            <div className="absolute inset-0 bg-black/20 group-hover:bg-black/10 transition-colors duration-500" />
                            <div className="absolute inset-0 flex items-center justify-center">
                                <h3 className="text-3xl md:text-4xl font-medium text-white drop-shadow-lg">Fullstack Developer</h3>
                            </div>
                            <div className="absolute bottom-6 right-6 w-10 h-10 bg-white/30 backdrop-blur rounded-full flex items-center justify-center text-white">
                                <ArrowRight size={16} />
                            </div>
                        </div>
                        <h4 className="text-xl font-medium mb-2">Fullstack Developer</h4>
                        <p className="text-[#5F6368]">
                            Build data-rich applications with ready-to-use APIs.
                            Focus on UX while XFlow handles the backend complexity.
                        </p>
                    </div>
                    {/* Card 5:Fullstack Developer */}
                    <div className="min-w-[85vw] md:min-w-[600px] snap-center">
                        <div className="aspect-[16/10] bg-[#FEF7E0] rounded-[32px] relative overflow-hidden group mb-6">
                            <img
                                src={sk}
                                alt="Fullstack Developer"
                                className="absolute inset-0 w-full h-full object-cover transition-transform duration-700 group-hover:scale-105"
                            />
                            <div className="absolute inset-0 bg-black/20 group-hover:bg-black/10 transition-colors duration-500" />
                            <div className="absolute inset-0 flex items-center justify-center">
                                <h3 className="text-3xl md:text-4xl font-medium text-white drop-shadow-lg">Full Stack overflow</h3>
                            </div>
                            <div className="absolute bottom-6 right-6 w-10 h-10 bg-white/30 backdrop-blur rounded-full flex items-center justify-center text-white">
                                <ArrowRight size={16} />
                            </div>
                        </div>
                        <h4 className="text-xl font-medium mb-2">Fullstack Developer</h4>
                        <p className="text-[#5F6368]">
                            Track product metrics and user behavior instantly.
                            Validate hypotheses with real-time data access.
                        </p>
                    </div>
                </div>
            </section>

            {/* CTA Footer Section */}
            <section className="py-32 px-6 text-center">
                {/* <h2 className="text-6xl md:text-8xl font-bold mb-12 tracking-tight">
                    XFlow
                </h2> */}
                <div className="flex flex-col sm:flex-row items-center justify-center gap-4">
                    <button
                        onClick={() => navigate("/login")}
                        className="bg-[#202124] text-white px-10 py-5 rounded-full text-xl font-medium hover:bg-gray-800 transition-all hover:scale-105"
                    >
                        Start XFlow Now
                    </button>
                </div>
            </section>

            {/* Footer (Minimal) */}
            <footer className="py-12 px-6 border-t border-gray-100 max-w-[1440px] mx-auto flex flex-col md:flex-row justify-between items-center gap-6">
                <div className="text-[100px] leading-none font-bold text-gray-100 select-none pointer-events-none absolute left-0 bottom-0 -z-10 opacity-50">
                    XFlow
                </div>
                <div className="flex items-center gap-2">
                    <img src={logo} alt="XFlow" className="h-6" />
                </div>
                <div className="flex gap-8 text-sm text-[#5F6368]">
                    <button className="hover:text-[#202124]">Privacy</button>
                    <button className="hover:text-[#202124]">Terms</button>
                    <button className="hover:text-[#202124]">Tel: 010-4819-4258</button>
                </div>
                <div className="text-sm text-[#9AA0A6]">
                    © 2026 XFlow
                </div>
            </footer>

        </div>
    );
}

export default LandingPage;
