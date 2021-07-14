package corrsketches.benchmark.utils;

import picocli.CommandLine;

public abstract class CliTool implements Runnable {

  @Override
  public void run() {
    try {
      execute();
    } catch (Exception e) {
      throw new RuntimeException("Failed to execute command.", e);
    }
  }

  public abstract void execute() throws Exception;

  public static void run(String[] args, CliTool tool) {
    System.exit(new CommandLine(tool).execute(args));
  }
}
