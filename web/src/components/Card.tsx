type CardProps = {
	title: string;
	content: string | number;
};

function Card({ title, content }: CardProps) {
	return (
		<div className="bg-white shadow-lg rounded-lg p-4 border border-neutral-200">
			<h2 className="text-xl font-bold">{title}</h2>
			<p className="text-gray-700">{content}</p>
		</div>
	);
}

export default Card;
