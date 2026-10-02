{ pkgs, ... }:

{
  languages.rust = {
    enable = true;
    channel = "stable";
  };

  packages = with pkgs; [
    cargo-nextest
    just
    rust-analyzer
  ];

  scripts.build.exec = "just build";
  scripts.check.exec = "just check";
  scripts.test.exec = "just test";

  enterShell = ''
    echo "Rust: $(rustc --version)"
    echo "Cargo: $(cargo --version)"
  '';

  enterTest = "just check";
}
