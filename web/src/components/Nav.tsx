import { Link } from "@tanstack/react-router";

function Nav() {
	return (
		<nav className="bg-black text-white p-4 font-semibold text-xl">
			<Link to="/">Codescene</Link>
		</nav>
	);
}

export default Nav;
