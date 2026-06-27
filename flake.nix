{
  description = "dj.recorder — durable, crash-safe persistence for native Clojure data structures";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs = { self, nixpkgs }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];
      forAllSystems = f:
        nixpkgs.lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});
    in {
      devShells = forAllSystems (pkgs: {
        default = pkgs.mkShell {
          # Pure-JVM toolchain only; the library deliberately avoids native deps.
          packages = [
            pkgs.temurin-bin   # JDK (LTS)
            pkgs.clojure       # clj / clojure CLI + tools.deps
            pkgs.babashka      # bb, used by clj-nrepl-eval
          ];
        };
      });
    };
}
