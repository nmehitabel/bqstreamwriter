{ pkgs, ... }:

{
  # https://devenv.sh/basics/
  env.GREET = "devenv";

  # https://devenv.sh/packages/
  packages = [ pkgs.git pkgs.google-cloud-sdk ];

  enterShell = ''
    git --version
    gcloud --version
    echo $JAVA_HOME
  '';

  # https://devenv.sh/languages/
  languages.nix.enable = true;
  languages.java.enable = true;
  languages.java.jdk.package = pkgs.jdk17;
  languages.kotlin.enable = true;
  services.mysql.enable = true;
  # https://devenv.sh/scripts/
  # scripts.hello.exec = "echo hello from $GREET";

  # https://devenv.sh/pre-commit-hooks/
  # pre-commit.hooks.shellcheck.enable = true;

  # https://devenv.sh/processes/
  # processes.ping.exec = "ping example.com";
}
