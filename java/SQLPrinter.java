/**
 *
 */
//package sqlPrinter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Maximilian Valenza
 *
 */
public class SQLPrinter<E> extends ArrayList<E> {
	private static final long serialVersionUID = 1L;
	List<String> unformattedStringList;

	public SQLPrinter() {
		super();
	}

	public void setUnformattedStringList(List<String> line) {
		unformattedStringList = line;
	}

	@Override
	public String toString() {
		Iterator<String> stringListIterator = unformattedStringList.iterator();
		StringBuilder stringBuilder = new StringBuilder();

		if (!stringListIterator.hasNext()) {
			return "";
		}

		// Iterate over the whole line until the end is reached, then return
		for (; ; ) {
			String element = stringListIterator.next();
			stringBuilder.append(element);
			if (!stringListIterator.hasNext())
				return stringBuilder.toString();
			stringBuilder.append(",");
		}
	}
}
