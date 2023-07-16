{ naersk, src, lib, pkg-config}:

naersk.buildPackage {
  pname = "foundation";
  version = "0.1.0";

  src = ./.;

  cargoSha256 = lib.fakeSha256;

  nativeBuildInputs = [ pkg-config ];
  buildInputs = [ ];

  meta = {
    description = "Foundation is a server which serves content website";
    homepage = "https://github.com/dd-ix/foundation";
  };
}