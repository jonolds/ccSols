import sol1.Sol1;
import sol2.Sol2;
import sol3.Sol3;
import sol4.Sol4;

public class SolsDriver {

	public static void main(String[] args) throws Exception {
		Sol1.run("sol1/input", "sol1/output");
		Sol2.run("sol2/input", "sol2/output");
		Sol3.run("sol3/input", "sol3/output");
		Sol4.run("sol4/input", "sol4/output");
	}
}