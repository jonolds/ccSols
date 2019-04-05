import sol1.Sol1;
import sol2.Sol2;
import sol3.Sol3;
import sol4.Sol4driver;
import sol5.Sol5driver;
import sol6.Sol6driver;

public class SolsDriver {

	public static void main(String[] args) throws Exception {
		Sol1.main1(new String[] {"sol1/input", "sol1/output"});
		Sol2.main2(new String[] {"sol2/input", "sol2/output"});
		Sol3.main3(new String[] {"sol3/input", "sol3/output"});
		Sol4driver.main4(new String[] {"sol4/input", "sol4/output"});
		Sol5driver.run(new String[] {"sol5/input", "sol5/output"});
		Sol6driver.run(new String[] {"-i", "8", "-m", "3", "-r", "3"});
	}
}