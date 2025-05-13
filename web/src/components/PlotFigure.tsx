import * as Plot from "@observablehq/plot";
import type { PlotOptions } from "@observablehq/plot";
import { useEffect, useRef, useState } from "react";

export default function PlotFigure({
	options,
	className,
}: { options: PlotOptions; className?: string }) {
	const plotref = useRef<HTMLDivElement>(null);
	const containerRef = useRef<HTMLDivElement>(null);
	const [width, setWidth] = useState<number | undefined>(undefined);

	useEffect(() => {
		if (!containerRef.current) return;

		const observer = new ResizeObserver(([entry]) => {
			setWidth(entry.contentRect.width);
		});
		observer.observe(containerRef.current);
		return () => observer.disconnect();
	}, []);

	useEffect(() => {
		if (width === undefined || !plotref.current) return;

		let plot: (HTMLElement | SVGSVGElement) & Plot.Plot;
		if ("width" in options) {
			plot = Plot.plot(options);
		} else {
			plot = Plot.plot({ ...options, width });
		}

		plotref.current.innerHTML = "";
		plotref.current.append(plot);
		return () => plot.remove();
	}, [options, width]);

	return (
		<div ref={containerRef} className={className}>
			<div ref={plotref} />
		</div>
	);
}
